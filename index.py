#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
阿里云盘备份插件（基础版）
- 扫码登录（明心安备开放平台）
- 测试文件上传

说明：
- 产品免费使用 如有分片备份需求可前往宝塔软件商店 - 第三方软件 - 阿里云盘备份插件购买，作者（旧楼雨）
- 本产品初衷仅用于备份需求，支持大家购买付费插件（更稳定 长期维护）
接口大致约定（符合宝塔插件习惯的 s 参数）：
- /plugin?action=a&name=aliyundrive_backup&s=get_qr_code        获取登录二维码信息
- /plugin?action=a&name=aliyundrive_backup&s=check_login_status 轮询登录结果
- /plugin?action=a&name=aliyundrive_backup&s=test_upload        测试上传一个小文件
"""

import os
import json
import time
import base64

try:
    import requests
    import urllib3
    # 禁用 SSL 警告
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    # 在宝塔环境中请确保已安装 requests（pip install requests）
    requests = None
    urllib3 = None


PLUGIN_NAME = "aliyundrive_backup"
PLUGIN_PATH = os.path.join("/www/server/panel/plugin", PLUGIN_NAME)
TOKEN_FILE = os.path.join(PLUGIN_PATH, "token.json")


BASE_URL = "https://openapi.alipan.com"  # 阿里云盘官方接口地址（用于文件上传）
PROXY_URL = "https://alipanbackup.openapi.apii.cn"  # 中转接口地址（用于授权登录）


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
    模拟宝塔的 public.returnMsg 结构，方便前端统一处理
    """
    res = {"status": status, "msg": msg}
    if data is not None:
        res["data"] = data
    return res


class aliyundrive_backup:
    """
    宝塔插件主类
    """

    def __init__(self):
        pass

    # ------------------ 登录相关 ------------------ #
    def get_qr_code(self, get):
        """
        获取扫码登录二维码
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")

        try:
            url = PROXY_URL + "/oauth/authorize/qrcode"
            resp = requests.post(url, json={}, timeout=10, verify=False)
            if resp.status_code != 200:
                return _public_return(False, "获取二维码失败，HTTP 状态码: {}".format(resp.status_code))

            result = resp.json()
            # 检查中转接口返回的状态
            if not result.get("status"):
                return _public_return(False, "获取二维码失败: {}".format(result.get("msg", "未知错误")))
            
            # 从中转接口的 data 中获取二维码信息
            data = result.get("data", {})
            
            # 尝试多种可能的字段名
            qr_url = (data.get("qrCodeUrl") or data.get("qr_code_url") or 
                     data.get("qr_code") or data.get("url") or 
                     data.get("qrUrl") or "")
            login_id = (data.get("sid") or data.get("login_id") or 
                       data.get("loginId") or "")

            if not qr_url or not login_id:
                return _public_return(False, "返回数据中缺少 qr_url 或 login_id，请对照文档调整解析字段。", result)

            # 前端展示用的二维码地址
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
        返回：登录状态；当登录成功时，写入 token.json
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")

        login_id = getattr(get, "login_id", None)
        if not login_id:
            return _public_return(False, "缺少 login_id 参数。")

        try:
            # 通过中转接口查询登录状态
            url = PROXY_URL + "/oauth/qrcode/{}/status".format(login_id)
            resp = requests.get(url, timeout=10, verify=False)
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
                token_payload = {
                    "code": auth_code,
                }
                t_resp = requests.post(token_url, json=token_payload, timeout=10, verify=False)
                if t_resp.status_code != 200:
                    return _public_return(False, "换取 access_token 失败，HTTP 状态码: {}".format(t_resp.status_code))

                t_result = t_resp.json()
                if not t_result.get("status"):
                    return _public_return(False, "换取 access_token 失败: {}".format(t_result.get("msg", "未知错误")))
                
                t_data = t_result.get("data", {})
                # 保存 token 信息
                save_res = _save_token(t_data)
                if not save_res["status"]:
                    return save_res

                return _public_return(True, "登录成功并保存 token。", {"login_status": status})

            # 非成功状态直接返回给前端做轮询展示
            return _public_return(True, "查询成功", {"login_status": status})
        except Exception as e:
            return _public_return(False, "查询登录状态异常: {}".format(e))

    # ------------------ 上传相关 ------------------ #
    def test_upload(self, get):
        """
        测试上传一个小文件到阿里云盘
        """
        if requests is None:
            return _public_return(False, "服务器未安装 requests 模块，请先安装后重试。")

        token_info = _load_token()
        if not token_info:
            return _public_return(False, "尚未登录，请先扫码登录。")

        access_token = token_info.get("access_token")
        if not access_token:
            return _public_return(False, "token 信息中缺少 access_token，请重新扫码登录。")

        # 测试文件内容
        test_content = "AliyunDrive Backup Plugin Test - {}".format(time.strftime("%Y-%m-%d %H:%M:%S"))
        file_name = "bt_aliyundrive_test_{}.txt".format(int(time.time()))

        try:
            headers = {
                "Authorization": "Bearer {}".format(access_token),
                "Content-Type": "application/json;charset=utf-8"
            }

            # 1. 创建文件 / 初始化上传
            # 实际 path / body 字段严格按照文档为准
            init_url = BASE_URL + "/adrive/v1.0/openFile/create"
            body = {
                "drive_id": token_info.get("default_drive_id"),
                "parent_file_id": "root",
                "name": file_name,
                "type": "file",
                "check_name_mode": "auto_rename",
                "size": len(test_content.encode("utf-8"))
            }
            init_resp = requests.post(init_url, headers=headers, json=body, timeout=10)
            if init_resp.status_code != 200:
                return _public_return(False, "初始化上传失败，HTTP 状态码: {}".format(init_resp.status_code))

            init_data = init_resp.json()
            upload_url = None

            # 文档里一般会给 part_info_list / upload_id / file_id 等信息
            part_info_list = init_data.get("part_info_list") or []
            if part_info_list:
                upload_url = part_info_list[0].get("upload_url")

            if not upload_url:
                return _public_return(False, "初始化返回中未找到 upload_url，请对照文档调整解析字段。", init_data)

            # 2. PUT 文件内容到 upload_url
            put_resp = requests.put(upload_url, data=test_content.encode("utf-8"), timeout=30)
            if put_resp.status_code not in (200, 201):
                return _public_return(False, "上传文件数据失败，HTTP 状态码: {}".format(put_resp.status_code))

            # 3. 调用 complete 完成上传
            complete_url = BASE_URL + "/adrive/v1.0/openFile/complete"
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


def index(get):
    """
    测试功能
    """
    token_info = _load_token()
    logined = bool(token_info and token_info.get("access_token"))
    return _public_return(True, "aliyundrive_backup 插件正常", {"logined": logined})

