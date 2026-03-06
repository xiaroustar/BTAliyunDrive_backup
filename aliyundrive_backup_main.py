#!/usr/bin/python
# coding: utf-8
# +-------------------------------------------------------------------
# | 宝塔Linux面板
# editor: Xiarou(15001904@qq.com tel:13132131321)
#+--------------------------------------------------------------------
#|   宝塔第三方应用 AliyunDrive_backup
#+--------------------------------------------------------------------

__all__ = ['aliyundrive_backup_main']

import sys
import os
import json
import time
import importlib.util
import uuid

basedir = os.path.abspath(os.path.dirname(__file__))
plugin_path = "/www/server/panel/plugin/aliyundrive_backup/"
try:
    os.chdir("/www/server/panel")
    sys.path.append("class/")
except Exception:
    os.chdir(os.path.join(basedir, '..', '..'))
    sys.path.append("class/")
    plugin_path = basedir.rstrip('/') + '/'

# 确保插件目录在 sys.path 中，方便直接 import mydb
if plugin_path not in sys.path:
    sys.path.append(plugin_path)

def _load_local_module(module_name, module_file):
    """
    (module_or_None, err_or_None)
    """
    try:
        if not os.path.exists(module_file):
            return None, "模块文件不存在: {}".format(module_file)
        spec = importlib.util.spec_from_file_location(module_name, module_file)
        if spec is None or spec.loader is None:
            return None, "无法创建 spec: {}".format(module_file)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod, None
    except Exception as e:
        return None, str(e)

try:
    import requests
    import urllib3
    # 禁用 SSL 警告
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    # 在宝塔环境中请确保已安装 requests（pip install requests）
    requests = None
    urllib3 = None

try:
    import public
except ImportError:
    public = None

mydb, _mydb_err = _load_local_module(
    "aliyundrive_backup_mydb",
    os.path.join(plugin_path.rstrip("/"), "mydb.py")
)


TOKEN_FILE = os.path.join(plugin_path, "token.json")
SERVER_ID_FILE = os.path.join(plugin_path, "server_id.json")


BASE_URL = "https://openapi.alipan.com"  # 阿里云盘官方接口地址（用于文件上传等）
PROXY_URL = "https://alipanbackup.openapi.apii.cn"  # 中转接口地址（用于授权登录）

def _get_server_id():
    """
    获取本服务器唯一标识（持久化 UUID）。
    - 同一台宝塔面板稳定不变
    - 不依赖 IP（避免 NAT/CDN/反代误判）
    """
    try:
        os.makedirs(os.path.dirname(SERVER_ID_FILE), exist_ok=True)
        if os.path.exists(SERVER_ID_FILE):
            with open(SERVER_ID_FILE, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
                sid = data.get("server_id")
                if sid and isinstance(sid, str):
                    sid = sid.strip()
                    if sid:
                        return sid
        sid = str(uuid.uuid4())
        with open(SERVER_ID_FILE, "w", encoding="utf-8") as f:
            json.dump({"server_id": sid, "create_time": int(time.time())}, f, ensure_ascii=False, indent=2)
        return sid
    except Exception:
        # 极端情况下退化：每次随机（不影响登录，只影响 viewer 聚合展示）
        return str(uuid.uuid4())


def _load_token():
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_token(data):
    try:
        os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        return {"status": False, "msg": "保存 token 失败: {}".format(e)}
    return {"status": True, "msg": "ok"}


def _public_return(status, msg, data=None):
    """
    """
    res = {"status": status, "msg": msg}
    if data is not None:
        res["data"] = data
    return res


class _DummySql(object):
    """当 mydb 加载失败时的降级对象，避免插件直接崩溃，并返回明确错误信息。"""
    def __init__(self, err):
        self._err = err or "未知错误"

    def table(self, table):
        self._table = table
        return self

    def where(self, *args, **kwargs):
        return self

    def field(self, *args, **kwargs):
        return self

    def order(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def select(self):
        return "error: mydb 加载失败: {}".format(self._err)

    def add(self, *args, **kwargs):
        return "error: mydb 加载失败: {}".format(self._err)

    def delete(self, *args, **kwargs):
        return "error: mydb 加载失败: {}".format(self._err)

    def find(self):
        return None


class aliyundrive_backup_main:
    """
    宝塔第三方应用主类
    """

    def __init__(self):
        if mydb is None or not hasattr(mydb, "Sql"):
            self._db = _DummySql(_mydb_err or "mydb.Sql 不存在")
        else:
            self._db = mydb.Sql()

    # ------------------ 通用 DB 工具 ------------------ #
    def Db(self, table):
        """
        获取 sqlite 表操作对象，目前主要用于：web (站点备份配置)
        """
        return self._db.table(table)

    # 内部工具：确保 token 中带有 default_drive_id（如没有，则调用 user/getDriveInfo 获取）
    def _ensure_drive_info(self, token_info):
        if not token_info:
            return None, _public_return(False, "尚未登录，请先扫码登录。")

        drive_id = token_info.get("default_drive_id") or token_info.get("effective_drive_id")
        if drive_id:
            return token_info, None

        access_token = token_info.get("access_token")
        if not access_token:
            return None, _public_return(False, "token 信息中缺少 access_token，请重新扫码登录。")

        if requests is None:
            return None, _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")

        try:
            url = BASE_URL + "/adrive/v1.0/user/getDriveInfo"
            headers = {
                "Authorization": "Bearer {}".format(access_token),
                "Content-Type": "application/json;charset=utf-8"
            }
            resp = requests.post(url, headers=headers, json={}, timeout=10)
            if resp.status_code != 200:
                try:
                    err_text = resp.text
                except Exception:
                    err_text = ""
                return None, _public_return(
                    False,
                    "获取用户 drive 信息失败，HTTP 状态码: {}，响应: {}".format(resp.status_code, err_text)
                )
            info = resp.json()
            # 缓存常用字段，方便后续使用
            default_drive_id = info.get("default_drive_id")
            backup_drive_id = info.get("backup_drive_id")
            resource_drive_id = info.get("resource_drive_id")
            folder_id = info.get("folder_id")

            token_info["default_drive_id"] = default_drive_id
            token_info["backup_drive_id"] = backup_drive_id
            token_info["resource_drive_id"] = resource_drive_id
            token_info["folder_id"] = folder_id

            # 计算一个“实际使用”的 drive：
            # - 若用户授权了某个文件夹（style=folder），该 folder 一般在 backup/resource 盘上
            #   优先使用 backup_drive_id / resource_drive_id；
            # - 否则，退回 default_drive_id。
            effective_drive_id = None
            if folder_id:
                effective_drive_id = backup_drive_id or resource_drive_id or default_drive_id
            else:
                effective_drive_id = default_drive_id or backup_drive_id or resource_drive_id
            token_info["effective_drive_id"] = effective_drive_id

            _save_token(token_info)
            return token_info, None
        except Exception as e:
            return None, _public_return(False, "获取用户 drive 信息异常: {}".format(e))

    # ------------------ 登录相关 ------------------ #
    def get_qr_code(self, get):
        """
        获取扫码登录二维码
        前端从返回的 qr_url / login_id 展示二维码并轮询状态。
        使用文档中的【手机扫码授权模式】接口：
          POST /oauth/authorize/qrcode
          返回 qrCodeUrl / sid
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")

        try:
            # 使用中转接口获取二维码
            url = PROXY_URL + "/oauth/authorize/qrcode"
            server_id = _get_server_id()
            payload = {
                "width": 430,
                "height": 430,
                "style": "folder",
                "server_id": server_id
            }
            headers = {"X-BT-Server-ID": server_id}
            resp = requests.post(url, json=payload, headers=headers, timeout=10, verify=False)
            if resp.status_code != 200:
                return _public_return(False, "获取二维码失败，HTTP 状态码: {}".format(resp.status_code))

            result = resp.json()
            print("中转接口返回的完整数据: {}".format(json.dumps(result, ensure_ascii=False)))
            
            # 检查中转接口返回的状态
            if not result.get("status"):
                return _public_return(False, "获取二维码失败: {}".format(result.get("msg", "未知错误")))
            
            # 从中转接口的 data 中获取二维码信息
            data = result.get("data", {})
            print("解析的 data 内容: {}".format(json.dumps(data, ensure_ascii=False)))
            
            # 尝试多种可能的字段名
            qr_url = (data.get("qrCodeUrl") or data.get("qr_code_url") or 
                     data.get("qr_code") or data.get("url") or 
                     data.get("qrUrl") or "")
            login_id = (data.get("sid") or data.get("login_id") or 
                       data.get("loginId") or "")

            print("解析结果 - qr_url: {}, login_id: {}".format(qr_url, login_id))

            if not qr_url or not login_id:
                return _public_return(False, "返回数据中缺少 qr_url 或 login_id，实际返回: {}".format(json.dumps(result, ensure_ascii=False)), result)

            res_data = {
                "qr_url": qr_url,
                "login_id": login_id,
                "expire": data.get("expire", 300)
            }
            return _public_return(True, "获取二维码成功", res_data)
        except Exception as e:
            return _public_return(False, "请求二维码接口异常: {}".format(e))

    def check_login_status(self, get):
        """
        轮询扫码登录状态
        入参：get.login_id
        登录成功后换取并保存 access_token / refresh_token 等。
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")

        login_id = getattr(get, "login_id", None)
        if not login_id:
            return _public_return(False, "缺少 login_id 参数。")

        try:
            # 使用中转接口查询登录状态
            url = PROXY_URL + "/oauth/qrcode/{}/status".format(login_id)
            server_id = _get_server_id()
            headers = {"X-BT-Server-ID": server_id}
            resp = requests.get(url, headers=headers, timeout=10, verify=False)
            if resp.status_code != 200:
                return _public_return(False, "查询登录状态失败，HTTP 状态码: {}".format(resp.status_code))

            result = resp.json()
            # 检查中转接口返回的状态
            if not result.get("status"):
                return _public_return(False, "查询登录状态失败: {}".format(result.get("msg", "未知错误")))
            
            data = result.get("data", {})
            # 文档状态枚举：WaitLogin / ScanSuccess / LoginSuccess / QRCodeExpired
            status = data.get("status")

            if status == "LoginSuccess":
                existing_token = _load_token()
                if existing_token and existing_token.get("access_token"):
                    return _public_return(
                        True,
                        "已登录，无需重复换取 token。",
                        {"login_status": status}
                    )

                # 登录成功时才会返回 authCode
                auth_code = data.get("authCode") or data.get("code")
                if not auth_code:
                    return _public_return(False, "登录成功但未拿到 auth_code，请对照文档调整字段。", data)

                # 通过中转接口获取 access_token
                token_url = PROXY_URL + "/oauth/access_token"
                server_id = _get_server_id()
                token_payload = {
                    "code": auth_code,
                    "grant_type": "authorization_code",
                    "server_id": server_id
                }
                headers = {"X-BT-Server-ID": server_id}
                t_resp = requests.post(token_url, json=token_payload, headers=headers, timeout=10, verify=False)
                if t_resp.status_code != 200:
                    # 把错误码和 message 一并返回，便于排查
                    err_msg = ""
                    try:
                        err_json = t_resp.json()
                        err_code = err_json.get("code")
                        err_message = err_json.get("message")
                        err_msg = "code={}, message={}".format(err_code, err_message)
                    except Exception:
                        try:
                            err_msg = t_resp.text
                        except Exception:
                            err_msg = ""
                    return _public_return(
                        False,
                        "换取 access_token 失败，HTTP 状态码: {}，响应: {}".format(t_resp.status_code, err_msg)
                    )

                token_result = t_resp.json()
                # 检查中转接口返回的状态
                if not token_result.get("status"):
                    return _public_return(False, "换取 access_token 失败: {}".format(token_result.get("msg", "未知错误")))
                
                t_data = token_result.get("data", {})
                if not t_data.get("access_token"):
                    return _public_return(False, "换取 access_token 失败: 返回数据中缺少 access_token")
                save_res = _save_token(t_data)
                if not save_res["status"]:
                    return save_res

                # 登录成功后，尝试预取一次 drive 信息，写入 default_drive_id
                _, drive_err = self._ensure_drive_info(t_data)
                if drive_err and not drive_err["status"]:
                    # 获取 drive 失败不阻断登录，只作为附加提示
                    return _public_return(
                        True,
                        "登录成功并保存 token，但获取 drive 信息时出错: {}".format(drive_err.get("msg")),
                        {"login_status": status}
                    )

                return _public_return(True, "登录成功并保存 token。", {"login_status": status})

            return _public_return(True, "查询成功", {"login_status": status, "raw": data})
        except Exception as e:
            return _public_return(False, "查询登录状态异常: {}".format(e))

    # ------------------ 上传相关 ------------------ #
    def test_upload(self, get):
        """
        测试上传一个小文件到阿里云盘。
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")

        token_info = _load_token()
        if not token_info:
            return _public_return(False, "尚未登录，请先扫码登录。")

        access_token = token_info.get("access_token")
        if not access_token:
            return _public_return(False, "token 信息中缺少 access_token，请重新扫码登录。")

        test_content = "AliyunDrive Backup Plugin Test - {}".format(time.strftime("%Y-%m-%d %H:%M:%S"))
        file_name = "bt_aliyundrive_test_{}.txt".format(int(time.time()))

        try:
            # 确保有 drive / folder 信息
            token_info, drive_err = self._ensure_drive_info(token_info)
            if drive_err and not drive_err["status"]:
                return drive_err

            headers = {
                "Authorization": "Bearer {}".format(access_token),
                "Content-Type": "application/json;charset=utf-8"
            }

            init_url = BASE_URL + "/adrive/v1.0/openFile/create"
            parent_file_id = getattr(get, "parent_file_id", None)
            if not parent_file_id:
                parent_file_id = token_info.get("folder_id") or "root"
            body = {
                "drive_id": token_info.get("effective_drive_id") or token_info.get("default_drive_id"),
                "parent_file_id": parent_file_id,
                "name": file_name,
                "type": "file",
                "check_name_mode": "auto_rename",
                "size": len(test_content.encode("utf-8"))
            }
            init_resp = requests.post(init_url, headers=headers, json=body, timeout=10)
            if init_resp.status_code != 200:
                # 尝试解析错误信息，便于排查
                err_msg = ""
                try:
                    err_json = init_resp.json()
                    err_code = err_json.get("code")
                    err_message = err_json.get("message")
                    err_msg = "code={}, message={}".format(err_code, err_message)
                except Exception:
                    try:
                        err_msg = init_resp.text
                    except Exception:
                        err_msg = ""
                return _public_return(
                    False,
                    "初始化上传失败，HTTP 状态码: {}，响应: {}".format(init_resp.status_code, err_msg)
                )

            init_data = init_resp.json()
            part_info_list = init_data.get("part_info_list") or []
            upload_url = part_info_list[0].get("upload_url") if part_info_list else None

            if not upload_url:
                return _public_return(False, "初始化返回中未找到 upload_url，请对照文档调整解析字段。", init_data)

            put_resp = requests.put(upload_url, data=test_content.encode("utf-8"), timeout=30)
            if put_resp.status_code not in (200, 201):
                return _public_return(False, "上传文件数据失败，HTTP 状态码: {}".format(put_resp.status_code))

            complete_url = BASE_URL + "/adrive/v1.0/openFile/complete"  # 占位
            complete_body = {
                "drive_id": init_data.get("drive_id"),
                "file_id": init_data.get("file_id"),
                "upload_id": init_data.get("upload_id")
            }
            complete_resp = requests.post(complete_url, headers=headers, json=complete_body, timeout=10)
            if complete_resp.status_code != 200:
                return _public_return(False, "完成上传失败，HTTP 状态码: {}".format(complete_resp.status_code))

            complete_data = complete_resp.json()
            return _public_return(True, "测试上传成功。", {"file_name": file_name, "result": complete_data})
        except Exception as e:
            return _public_return(False, "上传过程异常: {}".format(e))

    # ------------------ 列表相关 ------------------ #
    def list_root_files(self, get):
        """
        获取根目录（root）的文件列表，便于在面板上直接看到当前网盘文件。
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")

        token_info = _load_token()
        if not token_info:
            return _public_return(False, "尚未登录，请先扫码登录。")

        access_token = token_info.get("access_token")
        if not access_token:
            return _public_return(False, "token 信息中缺少 access_token，请重新扫码登录。")

        token_info, drive_err = self._ensure_drive_info(token_info)
        if drive_err and not drive_err["status"]:
            return drive_err

        drive_id = token_info.get("effective_drive_id") or token_info.get("default_drive_id")
        if not drive_id:
            return _public_return(False, "未获取到 drive_id，请稍后重试。")

        # 初次调用或未指定时，优先使用授权的 folder_id，其次 root
        parent_file_id = getattr(get, "parent_file_id", None)
        if not parent_file_id or parent_file_id in ("", "root"):
            parent_file_id = token_info.get("folder_id") or "root"

        try:
            headers = {
                "Authorization": "Bearer {}".format(access_token),
                "Content-Type": "application/json;charset=utf-8"
            }
            url = BASE_URL + "/adrive/v1.0/openFile/list"
            body = {
                "drive_id": drive_id,
                "parent_file_id": parent_file_id,
                "limit": 50,
                "order_by": "updated_at",
                "order_direction": "DESC",
                "fields": "*"
            }
            resp = requests.post(url, headers=headers, json=body, timeout=10)
            if resp.status_code != 200:
                try:
                    err_text = resp.text
                except Exception:
                    err_text = ""
                return _public_return(
                    False,
                    "获取文件列表失败，HTTP 状态码: {}，响应: {}".format(resp.status_code, err_text)
                )

            data = resp.json()
            return _public_return(True, "获取文件列表成功。", data)
        except Exception as e:
            return _public_return(False, "获取文件列表异常: {}".format(e))

    # ------------------ 站点备份相关 ------------------ #
    def web(self, get):
        """
        获取站点列表及是否启用备份状态，仿照 alipan 的 web 方法。
        """
        if public is None:
            return _public_return(False, "服务器缺少 public 模块，无法读取站点列表。")

        try:
            sites = public.M('sites').field('id,name,edate,path,status,ps').select()
        except Exception as e:
            return _public_return(False, "读取站点列表失败: {}".format(e))

        data = []
        for item in sites:
            tmp = dict(item)
            # 使用 select() 方法（兼容性更好）
            res_list = self.Db('web').where('sites_id=?', tmp['id']).select()
            if res_list and isinstance(res_list, list) and len(res_list) > 0:
                tmp['backup_status'] = 1
            else:
                tmp['backup_status'] = 0
            data.append(tmp)
        return _public_return(True, "获取站点列表成功。", {"sites": data})

    def sw_web(self, get):
        """
        开启/关闭某个站点的备份。
        参数：sites_id, status(1 开启, 0 关闭)
        """
        if public is None:
            return _public_return(False, "服务器缺少 public 模块，无法操作站点配置。")

        try:
            sites_id = int(get.sites_id)
            status = int(get.status)
        except Exception:
            return _public_return(False, "参数错误：sites_id 或 status 非数字。")

        if status == 1:
            # 开启备份：写入 web 表
            try:
                site = public.M('sites').where('id=?', sites_id).find()
                if not site:
                    return _public_return(False, "站点不存在。")
                date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                datas = (sites_id, site['name'], site['path'], date_time)
                self.Db('web').add('sites_id,name,path,create_time', datas)
                return _public_return(True, "已开启站点备份。")
            except Exception as e:
                return _public_return(False, "开启站点备份失败: {}".format(e))
        else:
            # 关闭备份：从 web 表删除
            try:
                self.Db('web').where('sites_id=?', sites_id).delete()
                return _public_return(True, "已关闭站点备份。")
            except Exception as e:
                return _public_return(False, "关闭站点备份失败: {}".format(e))

    # ------------------ 数据库备份相关 ------------------ #
    def db_bak(self, get):
        """
        获取数据库列表及是否启用备份状态，仿照 alipan 的 db_bak 方法。
        """
        if public is None:
            return _public_return(False, "服务器缺少 public 模块，无法读取数据库列表。")

        # 确保 database 表存在
        self._ensure_database_table()

        try:
            databases = public.M('databases').field('*').select()
        except Exception as e:
            return _public_return(False, "读取数据库列表失败: {}".format(e))

        # 确保 conf 表存在
        self._ensure_conf_table()
        
        # 获取 MySQL 配置
        port_list = self.Db('conf').where('key=?', 'port').select()
        mysql_root_list = self.Db('conf').where('key=?', 'mysql_root').select()
        
        port_val = '3306'
        if port_list and isinstance(port_list, list) and len(port_list) > 0:
            port_item = port_list[0]
            if isinstance(port_item, dict):
                port_val = port_item.get('val', '3306')
            elif isinstance(port_item, (list, tuple)) and len(port_item) > 1:
                port_val = port_item[1]  # 假设第二列是 val
        
        mysql_root_val = ''
        if mysql_root_list and isinstance(mysql_root_list, list) and len(mysql_root_list) > 0:
            mysql_root_item = mysql_root_list[0]
            if isinstance(mysql_root_item, dict):
                mysql_root_val = mysql_root_item.get('val', '')
            elif isinstance(mysql_root_item, (list, tuple)) and len(mysql_root_item) > 1:
                mysql_root_val = mysql_root_item[1]  # 假设第二列是 val

        data = []
        for item in databases:
            tmp = dict(item)
            try:
                # 使用 select() 方法获取所有匹配的记录，然后检查（兼容性更好）
                db_id = tmp.get('id')
                if not db_id:
                    tmp['backup_status'] = 0
                    data.append(tmp)
                    continue
                
                res_list = self.Db('database').where('databases_id=?', db_id).select()
                # get() 返回列表，检查是否有匹配的记录
                found = False
                if res_list and isinstance(res_list, list):
                    for res_item in res_list:
                        if isinstance(res_item, dict):
                            # 字典格式，检查 databases_id 字段
                            if res_item.get('databases_id') == db_id:
                                found = True
                                break
                        elif isinstance(res_item, (list, tuple)) and len(res_item) > 0:
                            # 列表/元组格式，假设第一列是 databases_id
                            if res_item[0] == db_id:
                                found = True
                                break
                
                tmp['backup_status'] = 1 if found else 0
            except Exception as e:
                # 查询出错，默认未开启
                import traceback
                print("检查数据库备份状态时出错 (db_id={}): {} - {}".format(tmp.get('id'), e, traceback.format_exc()))
                tmp['backup_status'] = 0
            data.append(tmp)
        
        return _public_return(True, "获取数据库列表成功。", {
            "database": data,
            "port": port_val,
            "mysql_root": mysql_root_val
        })

    def _ensure_database_table(self):
        """确保 database 表存在（兼容旧版本 mydb.py）"""
        try:
            import sqlite3
            db_file = os.path.join(plugin_path, 'aliyundrive_backup.db')
            if os.path.exists(db_file):
                conn = sqlite3.connect(db_file)
                c = conn.cursor()
                # 检查表是否存在
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='database'")
                if not c.fetchone():
                    # 表不存在，创建它
                    c.execute("""
                        CREATE TABLE IF NOT EXISTS database (
                            databases_id INTEGER PRIMARY KEY,
                            name TEXT,
                            ps TEXT,
                            create_time TEXT
                        );
                    """)
                    conn.commit()
                conn.close()
        except Exception as e:
            print("确保 database 表存在时出错: {}".format(e))

    def sw_db(self, get):
        """
        开启/关闭某个数据库的备份。
        参数：databases_id, status(1 开启, 0 关闭)
        """
        if public is None:
            return _public_return(False, "服务器缺少 public 模块，无法操作数据库配置。")

        # 确保 database 表存在
        self._ensure_database_table()

        try:
            databases_id = int(get.databases_id)
            status = int(get.status)
        except Exception:
            return _public_return(False, "参数错误：databases_id 或 status 非数字。")

        if status == 1:
            # 开启备份：写入 database 表
            try:
                db_one = public.M('databases').where('id=?', databases_id).find()
                if not db_one:
                    return _public_return(False, "数据库不存在。")
                date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                existing_list = self.Db('database').where('databases_id=?', databases_id).select()
                if existing_list and isinstance(existing_list, list) and len(existing_list) > 0:
                    # 已存在记录，先删除
                    self.Db('database').where('databases_id=?', databases_id).delete()
                # 添加新记录
                datas = (databases_id, db_one['name'], db_one.get('ps', ''), date_time)
                add_result = self.Db('database').add('databases_id,name,ps,create_time', datas)
                if isinstance(add_result, str) and add_result.startswith('error'):
                    return _public_return(False, "开启数据库备份失败: {}".format(add_result))
                return _public_return(True, "已开启数据库备份。")
            except Exception as e:
                import traceback
                return _public_return(False, "开启数据库备份失败: {} - {}".format(e, traceback.format_exc()))
        else:
            # 关闭备份：从 database 表删除
            try:
                deleted = self.Db('database').where('databases_id=?', databases_id).delete()
                if isinstance(deleted, str) and deleted.startswith('error'):
                    return _public_return(False, "关闭数据库备份失败: {}".format(deleted))
                # deleted 可能是 0（记录不存在）或大于 0（成功删除），都算成功
                return _public_return(True, "已关闭数据库备份。")
            except Exception as e:
                import traceback
                return _public_return(False, "关闭数据库备份失败: {} - {}".format(e, traceback.format_exc()))

    def _ensure_conf_table(self):
        """确保 conf 表存在（兼容旧版本 mydb.py）"""
        try:
            import sqlite3
            db_file = os.path.join(plugin_path, 'aliyundrive_backup.db')
            if os.path.exists(db_file):
                conn = sqlite3.connect(db_file)
                c = conn.cursor()
                # 检查表是否存在
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conf'")
                if not c.fetchone():
                    # 表不存在，创建它
                    c.execute("""
                        CREATE TABLE IF NOT EXISTS conf (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            key TEXT UNIQUE,
                            val TEXT
                        );
                    """)
                    conn.commit()
                conn.close()
        except Exception as e:
            print("确保 conf 表存在时出错: {}".format(e))

    def db_info(self, get):
        """
        保存 MySQL 配置信息（端口和 root 密码）
        参数：port, mysql_root
        """
        # 确保 conf 表存在
        self._ensure_conf_table()

        try:
            port = get.port if hasattr(get, 'port') else '3306'
            mysql_root = get.mysql_root if hasattr(get, 'mysql_root') else ''
            
            # 保存端口
            port_existing = self.Db('conf').where('key=?', 'port').select()
            if port_existing and isinstance(port_existing, list) and len(port_existing) > 0:
                # 更新：先删除旧记录
                self.Db('conf').where('key=?', 'port').delete()
            self.Db('conf').add('key,val', ('port', port))
            
            # 保存 root 密码
            mysql_root_existing = self.Db('conf').where('key=?', 'mysql_root').select()
            if mysql_root_existing and isinstance(mysql_root_existing, list) and len(mysql_root_existing) > 0:
                # 更新：先删除旧记录
                self.Db('conf').where('key=?', 'mysql_root').delete()
            self.Db('conf').add('key,val', ('mysql_root', mysql_root))
            
            return _public_return(True, "数据库配置已保存。")
        except Exception as e:
            import traceback
            return _public_return(False, "保存数据库配置失败: {} - {}".format(e, traceback.format_exc()))

    # ------------------ 偏好设置相关 ------------------ #
    def my_config(self, get):
        """
        获取偏好设置
        """
        # 确保 conf 表存在
        self._ensure_conf_table()
        
        try:
            # 获取各项配置
            site_keep = self._get_config_value('site_keep', '10')
            db_keep = self._get_config_value('db_keep', '10')
            ext_map = self._get_config_value('ext_map', '')
            exclude_dirs = self._get_config_value('exclude_dirs', '')
            
            # 打印调试信息
            print("读取配置 - site_keep: {}, db_keep: {}, ext_map: {}, exclude_dirs: {}".format(
                site_keep, db_keep, ext_map, exclude_dirs
            ))
            
            if site_keep is None or site_keep == '' or site_keep == 'None':
                site_keep = '10'
            else:
                try:
                    int(site_keep)  # 验证是否为数字，包括 0
                    # 如果是有效数字，保留原值（包括 '0'）
                except (ValueError, TypeError):
                    site_keep = '10'
            
            if db_keep is None or db_keep == '' or db_keep == 'None':
                db_keep = '10'
            else:
                try:
                    int(db_keep)  # 验证是否为数字，包括 0
                except (ValueError, TypeError):
                    db_keep = '10'
            
            # 确保文本字段不为 None
            if ext_map is None or ext_map == 'None':
                ext_map = ''
            if exclude_dirs is None or exclude_dirs == 'None':
                exclude_dirs = ''
            
            return _public_return(True, "获取配置成功", {
                "site_keep": str(site_keep),
                "db_keep": str(db_keep),
                "ext_map": str(ext_map) if ext_map else '',
                "exclude_dirs": str(exclude_dirs) if exclude_dirs else ''
            })
        except Exception as e:
            import traceback
            return _public_return(False, "获取配置失败: {} - {}".format(e, traceback.format_exc()))

    def save_config(self, get):
        """
        保存偏好设置
        """
        # 确保 conf 表存在
        self._ensure_conf_table()
        
        try:
            # 获取参数
            site_keep = get.site_keep if hasattr(get, 'site_keep') else None
            db_keep = get.db_keep if hasattr(get, 'db_keep') else None
            ext_map = get.ext_map if hasattr(get, 'ext_map') else ''
            exclude_dirs = get.exclude_dirs if hasattr(get, 'exclude_dirs') else ''
            
            # 转换为字符串，避免类型问题
            # 注意：'0' 是有效值（表示不限制），不应该被替换
            if site_keep is None:
                site_keep = '10'
            else:
                site_keep = str(site_keep).strip()
                if site_keep == '' or site_keep == 'None':
                    site_keep = '10'
            
            if db_keep is None:
                db_keep = '10'
            else:
                db_keep = str(db_keep).strip()
                if db_keep == '' or db_keep == 'None':
                    db_keep = '10'
            
            # 验证参数
            try:
                site_keep_int = int(site_keep)
                db_keep_int = int(db_keep)
                
                if site_keep_int < 0 or site_keep_int > 999:
                    return _public_return(False, "站点备份保留数量必须在 0-999 之间")
                if db_keep_int < 0 or db_keep_int > 999:
                    return _public_return(False, "数据库备份保留数量必须在 0-999 之间")
            except ValueError:
                return _public_return(False, "参数格式错误，数字字段必须为整数")
            
            # 保存配置
            configs = {
                'site_keep': str(site_keep),
                'db_keep': str(db_keep),
                'ext_map': str(ext_map) if ext_map else '',
                'exclude_dirs': str(exclude_dirs) if exclude_dirs else ''
            }
            
            # 打印调试信息
            print("保存配置 - site_keep: {}, db_keep: {}, ext_map: {}, exclude_dirs: {}".format(
                configs['site_keep'], configs['db_keep'], configs['ext_map'], configs['exclude_dirs']
            ))
            
            saved_count = 0
            # 使用 SQLite 的 INSERT OR REPLACE 语句，更可靠
            try:
                import sqlite3
                db_file = os.path.join(plugin_path, 'aliyundrive_backup.db')
                conn = sqlite3.connect(db_file)
                c = conn.cursor()
                
                for key, val in configs.items():
                    try:
                        # 使用 INSERT OR REPLACE 确保唯一性（conf 表的 key 字段是 UNIQUE）
                        c.execute("INSERT OR REPLACE INTO conf (key, val) VALUES (?, ?)", (key, val))
                        saved_count += 1
                        print("保存配置项 {} = {}".format(key, val))
                    except Exception as e:
                        print("保存配置 {} 时出错: {}".format(key, e))
                        import traceback
                        traceback.print_exc()
                
                conn.commit()
                conn.close()
                print("配置保存完成，共保存 {} 项".format(saved_count))
            except Exception as e:
                print("保存配置时数据库操作出错: {}".format(e))
                import traceback
                traceback.print_exc()
            
            if saved_count == len(configs):
                return _public_return(True, "配置保存成功（{} 项）".format(saved_count))
            else:
                return _public_return(False, "配置部分保存成功（{}/{} 项）".format(saved_count, len(configs)))
        except Exception as e:
            import traceback
            return _public_return(False, "保存配置失败: {} - {}".format(e, traceback.format_exc()))

    def _get_config_value(self, key, default=''):
        """
        获取配置值
        """
        try:
            # 确保 conf 表存在
            self._ensure_conf_table()
            res_list = self.Db('conf').where('key=?', key).select()
            print("获取配置值 {} - 查询结果: {}".format(key, res_list))
            
            if res_list and isinstance(res_list, list) and len(res_list) > 0:
                item = res_list[0]
                print("获取配置值 {} - 第一条记录: {}".format(key, item))
                
                if isinstance(item, dict):
                    val = item.get('val', default)
                    print("获取配置值 {} - 从字典获取 val: {}".format(key, val))
                    # 如果值是 None，返回默认值
                    if val is None:
                        return default
                    # 直接返回字符串形式，包括空字符串和 '0'
                    return str(val)
                elif isinstance(item, (list, tuple)):
                    if len(item) > 2:
                        val = item[2]  # 第三列是 val
                    elif len(item) > 1:
                        val = item[1]  # 如果只有两列，第二列是 val
                    else:
                        return default
                    print("获取配置值 {} - 从列表获取 val: {}".format(key, val))
                    if val is None:
                        return default
                    return str(val)
            print("获取配置值 {} - 未找到记录，返回默认值: {}".format(key, default))
            return default
        except Exception as e:
            print("获取配置值 {} 时出错: {}".format(key, e))
            import traceback
            traceback.print_exc()
            return default

    # ------------------ 计划任务相关 ------------------ #
    cron_title = u"[阿里云盘免费备份插件]守护任务(勿删)"

    def crontab(self, get):
        """
        检查计划任务状态
        """
        if public is None:
            return _public_return(False, "服务器缺少 public 模块，无法检查计划任务。")
        try:
            cron = public.M('crontab').where('name=?', self.cron_title).find()
            is_cron = 1 if cron else 0
            return _public_return(True, "查询成功", {"is_cron": is_cron, "cron_title": self.cron_title})
        except Exception as e:
            return _public_return(False, "检查计划任务失败: {}".format(e))

    def add_sync_task(self, get):
        """
        创建自动备份计划任务（每天 02:30 执行 service.py）
        """
        if public is None:
            return _public_return(False, "服务器缺少 public 模块，无法创建计划任务。")
        try:
            import crontab
            res = public.M('crontab').where('name=?', self.cron_title).find()
            if res:
                return _public_return(False, "无需重复设置!")
            data = {
                'name': self.cron_title,
                'type': 'day',
                'where1': '',
                'sBody': 'btpython /www/server/panel/plugin/aliyundrive_backup/service.py',
                'backupTo': 'localhost',
                'sType': 'toShell',
                'hour': 2,
                'minute': 30,
                'week': '',
                'sName': '',
                'urladdress': '',
                'save': ''
            }
            crontab.crontab().AddCrontab(data)
            if hasattr(public, 'WriteLog'):
                public.WriteLog('[阿里云盘备份]插件', '新增定时守护任务')
            return _public_return(True, "操作成功!")
        except Exception as e:
            return _public_return(False, "创建计划任务失败: {}".format(e))

    def stop_sync_task(self, get):
        """
        停止自动备份计划任务
        """
        if public is None:
            return _public_return(False, "服务器缺少 public 模块，无法删除计划任务。")
        try:
            import crontab
            id = public.M('crontab').where('name=?', self.cron_title).getField('id')
            if id:
                crontab.crontab().DelCrontab({'id': id})
            return _public_return(True, "操作成功!")
        except Exception as e:
            return _public_return(False, "删除计划任务失败: {}".format(e))

    def start_task(self, get):
        """
        手动执行一次备份队列
        """
        if public is None:
            return _public_return(False, "服务器缺少 public 模块，无法创建任务。")
        try:
            import panelTask
            t = panelTask.bt_task()
            task_shell = "btpython /www/server/panel/plugin/aliyundrive_backup/service.py"
            t.create_task('阿里云盘手动上传队列', 0, task_shell)
            return _public_return(True, "队列开始执行")
        except Exception as e:
            return _public_return(False, "创建手动任务失败: {}".format(e))

    # ------------------ 文件压缩相关 ------------------ #
    def get_time(self, format_str="%Y-%m-%d %H:%M:%S"):
        """获取当前时间字符串"""
        return time.strftime(format_str, time.localtime())

    def GetFileList(self, path, file_list, ext_map=None, exclude_dirs=None):
        """
        递归获取目录下所有文件（排除指定扩展名和目录）
        ext_map: 排除的扩展名列表，如 ['log', 'png']
        exclude_dirs: 排除的目录名列表，如 ['node_modules', '.git']
        """
        if ext_map is None:
            ext_map = []
        if exclude_dirs is None:
            exclude_dirs = []
        try:
            if not os.path.exists(path):
                print("路径不存在: {}".format(path))
                return
            if not os.path.isdir(path):
                print("路径不是目录: {}".format(path))
                return
            files = os.listdir(path)
            for file in files:
                file_path = os.path.join(path, file)
                try:
                    if os.path.isdir(file_path):
                        # 检查是否在排除目录列表中
                        dir_name = os.path.basename(file_path)
                        if dir_name in exclude_dirs:
                            continue
                        # 递归处理子目录
                        self.GetFileList(file_path, file_list, ext_map, exclude_dirs)
                    else:
                        # 只添加文件，排除指定扩展名
                        ext = os.path.splitext(file)[1].lstrip('.')
                        if ext in ext_map:
                            continue
                        file_list.append(file_path)
                except (OSError, PermissionError) as e:
                    # 跳过无权限访问的文件/目录
                    try:
                        safe_path = file_path.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                        safe_msg = str(e).encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                        print("跳过无权限访问的路径: {} ({})".format(safe_path, safe_msg))
                    except:
                        print("跳过无权限访问的路径")
                    continue
        except (OSError, PermissionError) as e:
            try:
                safe_path = path.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                safe_msg = str(e).encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                print("GetFileList 访问路径 {} 时出错: {}".format(safe_path, safe_msg))
            except:
                print("GetFileList 访问路径时出错")
        except Exception as e:
            try:
                safe_msg = str(e).encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                print("GetFileList 异常: {}".format(safe_msg))
            except:
                print("GetFileList 异常")
            try:
                import traceback
                traceback.print_exc()
            except:
                pass

    def Zip(self, sfile, dfile, ext_map=None, exclude_dirs=None):
        """
        压缩文件或目录为 zip
        sfile: 源文件/目录路径
        dfile: 目标 zip 文件路径
        ext_map: 排除的扩展名列表（可选）
        exclude_dirs: 排除的目录名列表（可选）
        """
        try:
            import zipfile
            filelists = []
            if os.path.isdir(sfile):
                # 获取目录下所有文件（应用过滤规则）
                self.GetFileList(sfile, filelists, ext_map, exclude_dirs)
                # 使用绝对路径，确保路径替换正确
                base_path = os.path.abspath(sfile)
            else:
                # 单个文件
                base_path = os.path.dirname(os.path.abspath(sfile))
                filelists.append(os.path.abspath(sfile))

            if not filelists:
                print("警告：没有找到需要压缩的文件（目录可能为空或无权限访问）")
                return False

            print("找到 {} 个文件需要压缩".format(len(filelists)))

            # 确保目标目录存在
            dfile_dir = os.path.dirname(dfile)
            if dfile_dir and not os.path.exists(dfile_dir):
                try:
                    os.makedirs(dfile_dir)
                except Exception as e:
                    print("创建目标目录失败: {}".format(e))
                    return False

            # 检查目标文件是否可写
            try:
                # 先尝试创建文件
                test_f = open(dfile, 'wb')
                test_f.close()
                os.remove(dfile)
            except Exception as e:
                print("无法写入目标文件 {}: {}".format(dfile, e))
                return False

            f = zipfile.ZipFile(dfile, 'w', zipfile.ZIP_DEFLATED)
            success_count = 0
            skip_count = 0
            for item in filelists:
                if os.path.isfile(item):
                    # 计算在 zip 中的相对路径
                    item_abs = os.path.abspath(item)
                    # 移除基础路径，保留相对路径
                    arcname = os.path.relpath(item_abs, base_path)
                    # 确保使用正斜杠（zip 标准）
                    arcname = arcname.replace('\\', '/')
                    # 移除开头的斜杠
                    if arcname.startswith('/'):
                        arcname = arcname[1:]
                    
                    # 清理文件名中的无效 UTF-8 字符（surrogate pairs）
                    # 移除所有 surrogate 字符（U+D800 到 U+DFFF）
                    safe_arcname = ''.join(
                        char for char in arcname 
                        if not (0xD800 <= ord(char) <= 0xDFFF)
                    )
                    # 确保可以编码为 UTF-8
                    try:
                        safe_arcname.encode('utf-8')
                    except UnicodeEncodeError:
                        # 如果仍有问题，使用 replace 策略
                        safe_arcname = safe_arcname.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                        # 再次移除 surrogate
                        safe_arcname = ''.join(
                            char for char in safe_arcname 
                            if not (0xD800 <= ord(char) <= 0xDFFF)
                        )
                    
                    try:
                        f.write(item, safe_arcname)
                        success_count += 1
                    except (UnicodeEncodeError, UnicodeDecodeError) as e:
                        # 编码错误，跳过该文件
                        skip_count += 1
                        try:
                            safe_item = item.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                            print("跳过无法编码的文件: {}".format(safe_item))
                        except:
                            print("跳过无法编码的文件（文件名包含无效字符）")
                        continue
                    except Exception as e:
                        # 其他错误
                        skip_count += 1
                        try:
                            safe_item = item.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                            safe_msg = str(e).encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                            print("压缩文件 {} 时出错: {}".format(safe_item, safe_msg))
                        except:
                            print("压缩文件时出错（无法显示文件名）")
                        continue
            f.close()
            
            # 确保文件已写入磁盘
            import sys
            if hasattr(sys, 'stdout'):
                sys.stdout.flush()
            
            # 验证文件确实存在且大小大于 0
            if not os.path.exists(dfile):
                print("错误：压缩文件创建失败，文件不存在: {}".format(dfile))
                return False
            
            file_size = os.path.getsize(dfile)
            if file_size == 0:
                print("错误：压缩文件大小为 0: {}".format(dfile))
                try:
                    os.remove(dfile)
                except:
                    pass
                return False
            
            if skip_count > 0:
                print("警告：跳过了 {} 个无法编码的文件".format(skip_count))
            
            if success_count == 0:
                print("警告：没有成功压缩任何文件")
                try:
                    os.remove(dfile)
                except:
                    pass
                return False
            
            # 使用规范化路径打印
            normalized_path = os.path.abspath(dfile)
            print("成功压缩 {} 个文件到 {}，文件大小: {} 字节".format(success_count, normalized_path, file_size))
            return True
        except Exception as e:
            try:
                safe_msg = str(e).encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                print("Zip 压缩异常: {}".format(safe_msg))
            except:
                print("Zip 压缩异常（无法显示错误信息）")
            try:
                import traceback
                traceback.print_exc()
            except:
                pass
            return False

    # ------------------ 文件管理相关 ------------------ #
    def list_files_in_folder(self, parent_file_id, drive_id=None, file_type='file'):
        """
        列出指定目录下的文件
        parent_file_id: 父目录 ID
        drive_id: drive_id（可选）
        file_type: 'file' 或 'folder'，默认 'file'
        返回: (success, msg, files_list)
        """
        if requests is None:
            return False, "服务器未安装 requests 模块", None
        
        token_info = _load_token()
        if not token_info:
            return False, "尚未登录，请先扫码登录", None
        
        access_token = token_info.get("access_token")
        if not access_token:
            return False, "token 信息中缺少 access_token", None
        
        token_info, drive_err = self._ensure_drive_info(token_info)
        if drive_err and not drive_err["status"]:
            return False, drive_err.get("msg"), None
        
        if drive_id is None:
            drive_id = token_info.get("effective_drive_id") or token_info.get("default_drive_id")
        
        if not drive_id:
            return False, "未获取到 drive_id", None
        
        try:
            headers = {
                "Authorization": "Bearer {}".format(access_token),
                "Content-Type": "application/json;charset=utf-8"
            }
            
            list_url = BASE_URL + "/adrive/v1.0/openFile/list"
            list_body = {
                "drive_id": drive_id,
                "parent_file_id": parent_file_id,
                "limit": 100,
                "type": file_type,
                "order_by": "updated_at",
                "order_direction": "DESC"
            }
            
            list_resp = requests.post(list_url, headers=headers, json=list_body, timeout=30)
            if list_resp.status_code != 200:
                err_msg = ""
                try:
                    err_json = list_resp.json()
                    err_code = err_json.get("code")
                    err_message = err_json.get("message")
                    err_msg = "code={}, message={}".format(err_code, err_message)
                except:
                    err_msg = list_resp.text
                return False, "列出文件失败，HTTP 状态码: {}，响应: {}".format(list_resp.status_code, err_msg), None
            
            list_data = list_resp.json()
            items = list_data.get("items", [])
            return True, "获取文件列表成功", items
        except Exception as e:
            return False, "列出文件异常: {}".format(e), None

    def delete_file_in_drive(self, file_id, drive_id=None):
        """
        删除云盘中的文件
        file_id: 要删除的文件 ID
        drive_id: drive_id（可选）
        返回: (success, msg)
        """
        if requests is None:
            return False, "服务器未安装 requests 模块"
        
        token_info = _load_token()
        if not token_info:
            return False, "尚未登录，请先扫码登录"
        
        access_token = token_info.get("access_token")
        if not access_token:
            return False, "token 信息中缺少 access_token"
        
        token_info, drive_err = self._ensure_drive_info(token_info)
        if drive_err and not drive_err["status"]:
            return False, drive_err.get("msg")
        
        if drive_id is None:
            drive_id = token_info.get("effective_drive_id") or token_info.get("default_drive_id")
        
        if not drive_id:
            return False, "未获取到 drive_id"
        
        try:
            headers = {
                "Authorization": "Bearer {}".format(access_token),
                "Content-Type": "application/json;charset=utf-8"
            }
            
            # 阿里云盘删除文件 API
            delete_url = BASE_URL + "/adrive/v1.0/openFile/recyclebin/trash"
            delete_body = {
                "drive_id": drive_id,
                "file_id": file_id
            }
            
            delete_resp = requests.post(delete_url, headers=headers, json=delete_body, timeout=30)
            if delete_resp.status_code != 200:
                err_msg = ""
                try:
                    err_json = delete_resp.json()
                    err_code = err_json.get("code")
                    err_message = err_json.get("message")
                    err_msg = "code={}, message={}".format(err_code, err_message)
                except:
                    err_msg = delete_resp.text
                return False, "删除文件失败，HTTP 状态码: {}，响应: {}".format(delete_resp.status_code, err_msg)
            
            return True, "删除文件成功"
        except Exception as e:
            return False, "删除文件异常: {}".format(e)

    def cleanup_old_backups(self, parent_file_id, keep_count, file_prefix, drive_id=None):
        """
        清理旧的备份文件，只保留最新的 keep_count 个
        parent_file_id: 父目录 ID
        keep_count: 保留数量（0=不限制）
        file_prefix: 文件名前缀（如 "站点名_web_" 或 "数据库名_db_"）
        drive_id: drive_id（可选）
        返回: (deleted_count, msg)
        """
        if keep_count <= 0:
            return 0, "备份数量限制为 0（不限制），跳过清理"
        
        success, msg, files = self.list_files_in_folder(parent_file_id, drive_id, file_type='file')
        if not success:
            return 0, "无法列出文件，跳过清理: {}".format(msg)
        
        if not files or len(files) <= keep_count:
            return 0, "文件数量未超过限制，无需清理"
        
        # 过滤出匹配前缀的文件
        matching_files = []
        for file in files:
            file_name = file.get("name", "")
            if file_name.startswith(file_prefix):
                matching_files.append(file)
        
        if len(matching_files) <= keep_count:
            return 0, "匹配文件数量未超过限制，无需清理"
        
        # 按更新时间排序（已从 API 返回 DESC 排序，但为了保险再排一次）
        matching_files.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
        
        # 删除超过限制的文件
        files_to_delete = matching_files[keep_count:]
        deleted_count = 0
        for file in files_to_delete:
            file_id = file.get("file_id")
            file_name = file.get("name", "")
            if file_id:
                success, msg = self.delete_file_in_drive(file_id, drive_id)
                if success:
                    deleted_count += 1
                    print("已删除旧备份文件: {}".format(file_name))
                else:
                    print("删除旧备份文件失败 {}: {}".format(file_name, msg))
        
        return deleted_count, "已清理 {} 个旧备份文件".format(deleted_count)

    # ------------------ 上传文件到阿里云盘 ------------------ #
    def upload_file_to_drive(self, local_file_path, parent_file_id=None, file_name=None):
        """
        上传本地文件到阿里云盘
        local_file_path: 本地文件路径
        parent_file_id: 父目录 ID（默认使用授权目录）
        file_name: 云盘中的文件名（默认使用本地文件名）
        返回: (success, msg, result_data)
        """
        if requests is None:
            return False, "服务器未安装 requests 模块", None

        token_info = _load_token()
        if not token_info:
            return False, "尚未登录，请先扫码登录", None

        access_token = token_info.get("access_token")
        if not access_token:
            return False, "token 信息中缺少 access_token", None

        token_info, drive_err = self._ensure_drive_info(token_info)
        if drive_err and not drive_err["status"]:
            return False, drive_err.get("msg"), None

        if not os.path.exists(local_file_path):
            return False, "本地文件不存在: {}".format(local_file_path), None

        if file_name is None:
            file_name = os.path.basename(local_file_path)

        if parent_file_id is None:
            parent_file_id = token_info.get("folder_id") or "root"

        drive_id = token_info.get("effective_drive_id") or token_info.get("default_drive_id")
        if not drive_id:
            return False, "未获取到 drive_id", None

        try:
            file_size = os.path.getsize(local_file_path)
            headers = {
                "Authorization": "Bearer {}".format(access_token),
                "Content-Type": "application/json;charset=utf-8"
            }

            # 1. 初始化上传
            init_url = BASE_URL + "/adrive/v1.0/openFile/create"
            body = {
                "drive_id": drive_id,
                "parent_file_id": parent_file_id,
                "name": file_name,
                "type": "file",
                "check_name_mode": "auto_rename",
                "size": file_size
            }
            init_resp = requests.post(init_url, headers=headers, json=body, timeout=30)
            if init_resp.status_code != 200:
                err_msg = ""
                try:
                    err_json = init_resp.json()
                    err_code = err_json.get("code")
                    err_message = err_json.get("message")
                    err_msg = "code={}, message={}".format(err_code, err_message)
                except Exception:
                    err_msg = init_resp.text
                return False, "初始化上传失败，HTTP 状态码: {}，响应: {}".format(init_resp.status_code, err_msg), None

            init_data = init_resp.json()
            part_info_list = init_data.get("part_info_list") or []
            upload_url = part_info_list[0].get("upload_url") if part_info_list else None

            if not upload_url:
                return False, "初始化返回中未找到 upload_url", None

            # 2. PUT 文件内容
            with open(local_file_path, 'rb') as f:
                put_resp = requests.put(upload_url, data=f, timeout=300)
                if put_resp.status_code not in (200, 201):
                    return False, "上传文件数据失败，HTTP 状态码: {}".format(put_resp.status_code), None

            # 3. 完成上传
            complete_url = BASE_URL + "/adrive/v1.0/openFile/complete"
            complete_body = {
                "drive_id": init_data.get("drive_id"),
                "file_id": init_data.get("file_id"),
                "upload_id": init_data.get("upload_id")
            }
            complete_resp = requests.post(complete_url, headers=headers, json=complete_body, timeout=30)
            if complete_resp.status_code != 200:
                return False, "完成上传失败，HTTP 状态码: {}".format(complete_resp.status_code), None

            complete_data = complete_resp.json()
            return True, "上传成功", complete_data
        except Exception as e:
            return False, "上传过程异常: {}".format(e), None

    # ------------------ 日志相关 ------------------ #
    def add_log(self, title, content):
        """添加日志记录"""
        try:
            date_time = self.get_time()
            self.Db('log').add('title,content,create_time', (title, content, date_time))
        except Exception as e:
            pass

    def get_log(self, get):
        """获取上传日志"""
        try:
            db_obj = self.Db('log')
            logs = []
            
            # 检查方法是否存在，使用降级方案（统一使用 select()，不依赖 get()）
            has_order = hasattr(db_obj, 'order')
            has_select = hasattr(db_obj, 'select')
            has_limit = hasattr(db_obj, 'limit')
            
            try:
                if has_order and has_select:
                    # 完整功能：使用 order 和 select
                    logs = db_obj.order('id DESC').limit(100).select()
                elif has_select and has_limit:
                    # 降级方案2：使用 select 和 limit
                    logs = db_obj.limit(100).select()
                    if isinstance(logs, list) and len(logs) > 0:
                        try:
                            # 尝试按 id 降序排序（假设第一列是 id）
                            logs.sort(key=lambda x: x[0] if isinstance(x, (list, tuple)) and len(x) > 0 else 0, reverse=True)
                        except:
                            pass
                elif has_select:
                    # 降级方案3：只使用 select，获取所有数据
                    logs = db_obj.select()
                    if isinstance(logs, list) and len(logs) > 100:
                        logs = logs[:100]  # 只取前100条
                else:
                    # 最后的降级方案：直接执行 SQL
                    import sqlite3
                    db_file = os.path.join(plugin_path, 'aliyundrive_backup.db')
                    if os.path.exists(db_file):
                        conn = sqlite3.connect(db_file)
                        conn.row_factory = sqlite3.Row
                        cursor = conn.cursor()
                        cursor.execute("SELECT * FROM log ORDER BY id DESC LIMIT 100")
                        rows = cursor.fetchall()
                        logs = [dict(row) for row in rows]
                        conn.close()
            except Exception as e:
                # 如果所有方法都失败，尝试直接执行 SQL
                try:
                    import sqlite3
                    db_file = os.path.join(plugin_path, 'aliyundrive_backup.db')
                    if os.path.exists(db_file):
                        conn = sqlite3.connect(db_file)
                        conn.row_factory = sqlite3.Row
                        cursor = conn.cursor()
                        cursor.execute("SELECT * FROM log ORDER BY id DESC LIMIT 100")
                        rows = cursor.fetchall()
                        logs = [dict(row) for row in rows]
                        conn.close()
                except Exception as sql_e:
                    return _public_return(False, "获取日志失败: 数据库方法不可用，且直接 SQL 查询也失败: {}".format(str(sql_e)))
            
            # 确保返回的是列表格式
            if not isinstance(logs, list):
                logs = []
            
            # 确保日志数据格式正确（转换为字典列表）
            formatted_logs = []
            for log in logs:
                if isinstance(log, dict):
                    formatted_logs.append(log)
                elif isinstance(log, (list, tuple)):
                    try:
                        # 假设列顺序：id, title, content, create_time
                        if len(log) >= 4:
                            formatted_logs.append({
                                'id': log[0],
                                'title': log[1],
                                'content': log[2],
                                'create_time': log[3]
                            })
                    except:
                        pass
            
            return _public_return(True, "获取日志成功", {"log": formatted_logs})
        except Exception as e:
            import traceback
            err_msg = str(e)
            try:
                # 尝试获取更详细的错误信息
                tb = traceback.format_exc()
                if 'get' in tb.lower() and 'attribute' in tb.lower():
                    err_msg = "数据库模块缺少 get 方法，请重新打包插件并更新 mydb.py 文件。当前使用降级方案。"
                elif 'order' in tb.lower():
                    err_msg = "数据库模块缺少 order 方法，请重新打包插件并更新 mydb.py 文件"
            except:
                pass
            return _public_return(False, "获取日志失败: {}".format(err_msg))

    def remove_log(self, get):
        """清空日志"""
        try:
            self.Db('log').where('id>?', 0).delete()
            return _public_return(True, "操作成功")
        except Exception as e:
            return _public_return(False, "清空日志失败: {}".format(e))

    # ------------------ 退出登录 ------------------ #
    def logout(self, get):
        """退出登录，删除 token 文件"""
        try:
            if os.path.exists(TOKEN_FILE):
                os.remove(TOKEN_FILE)
                return _public_return(True, "已退出登录")
            else:
                return _public_return(True, "当前未登录")
        except Exception as e:
            return _public_return(False, "退出登录失败: {}".format(e))

    # ------------------ 默认入口 ------------------ #
    def index(self, get):
        """
        在面板中可用于检测插件是否正常，以及是否已登录。
        """
        token_info = _load_token()
        logined = bool(token_info and token_info.get("access_token"))
        
        # 读取版本信息
        version = "1.0"  # 默认版本
        try:
            import json
            info_file = os.path.join(plugin_path, "info.json")
            if os.path.exists(info_file):
                with open(info_file, "r", encoding="utf-8") as f:
                    info_data = json.load(f)
                    version = info_data.get("versions", version)
        except:
            pass
        
        return _public_return(True, "aliyundrive_backup 插件正常", {
            "logined": logined,
            "version": version
        })

    def get_dev_info(self, get):
        """
        获取开发人员及赞助商信息（通过中转接口）
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")
        
        try:
            url = PROXY_URL + "/dev_info/"
            resp = requests.get(url, timeout=10, verify=False)
            if resp.status_code != 200:
                return _public_return(False, "获取开发人员信息失败，HTTP 状态码: {}".format(resp.status_code))
            
            result = resp.json()
            if not result.get("status"):
                return _public_return(False, "获取开发人员信息失败: {}".format(result.get("msg", "未知错误")))
            
            data = result.get("data", [])
            return _public_return(True, "获取开发人员信息成功", data)
        except Exception as e:
            return _public_return(False, "请求开发人员信息接口异常: {}".format(e))

    def get_version(self, get):
        """
        获取版本信息（通过中转接口）
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")
        
        try:
            url = PROXY_URL + "/version/"
            resp = requests.get(url, timeout=10, verify=False)
            if resp.status_code != 200:
                return _public_return(False, "获取版本信息失败，HTTP 状态码: {}".format(resp.status_code))
            
            result = resp.json()
            if not result.get("status"):
                return _public_return(False, "获取版本信息失败: {}".format(result.get("msg", "未知错误")))
            
            data = result.get("data", {})
            return _public_return(True, "获取版本信息成功", data)
        except Exception as e:
            return _public_return(False, "请求版本信息接口异常: {}".format(e))

