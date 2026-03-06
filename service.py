#!/usr/bin/python
# coding: utf-8
# 阿里云盘备份服务脚本（由计划任务调用）

import sys
import os
import time

basedir = os.path.abspath(os.path.dirname(__file__))
plugin_path = "/www/server/panel/plugin/aliyundrive_backup/"
try:
    os.chdir("/www/server/panel")
    sys.path.append("class/")
except Exception:
    os.chdir(os.path.join(basedir, '..', '..'))
    sys.path.append("class/")
    plugin_path = basedir.rstrip('/') + '/'

if plugin_path not in sys.path:
    sys.path.append(plugin_path)

try:
    import public
except ImportError:
    public = None

try:
    import requests
except ImportError:
    print("错误：未安装 requests 模块，请先执行: pip install requests")
    sys.exit(1)

try:
    from aliyundrive_backup_main import aliyundrive_backup_main
except ImportError:
    print("无法导入 aliyundrive_backup_main 模块")
    sys.exit(1)

# 规范化路径，避免双斜杠
runtime_path = os.path.join(plugin_path.rstrip('/'), 'runtime')

def ensure_runtime_dir():
    """确保运行时目录存在"""
    if not os.path.exists(runtime_path):
        os.makedirs(runtime_path)

def cleanup_runtime_dir():
    """清理运行时目录"""
    try:
        import shutil
        if os.path.exists(runtime_path):
            for item in os.listdir(runtime_path):
                item_path = os.path.join(runtime_path, item)
                if os.path.isfile(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
    except Exception as e:
        print("清理运行时目录失败: {}".format(e))

def ensure_folder_in_drive(backup_main, parent_file_id, folder_name, drive_id=None, access_token=None):
    """
    确保云盘中存在指定文件夹，返回其 file_id
    parent_file_id: 父目录 ID
    folder_name: 文件夹名称
    drive_id: drive_id（可选，如果不提供则从 token 获取）
    access_token: access_token（可选，如果不提供则从 token 获取）
    """
    import json
    import requests
    
    # 如果未提供 drive_id 或 access_token，从 token 文件获取
    if not access_token or not drive_id:
        token_file = os.path.join(plugin_path, "token.json")
        if not os.path.exists(token_file):
            print("无法获取 token 信息，请先登录")
            return None
        try:
            with open(token_file, "r", encoding="utf-8") as f:
                token_info = json.load(f)
        except Exception as e:
            print("读取 token 文件失败: {}".format(e))
            return None
        if not token_info:
            return None
        token_info, drive_err = backup_main._ensure_drive_info(token_info)
        if not token_info or (drive_err and not drive_err.get("status")):
            return None
        access_token = access_token or token_info.get("access_token")
        drive_id = drive_id or (token_info.get("effective_drive_id") or token_info.get("default_drive_id"))
    
    if not access_token or not drive_id:
        return None
    
    BASE_URL = "https://openapi.alipan.com"
    headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Content-Type": "application/json;charset=utf-8"
    }
    
    try:
        # 先列出当前目录，看是否已有该文件夹
        list_url = BASE_URL + "/adrive/v1.0/openFile/list"
        list_body = {
            "drive_id": drive_id,
            "parent_file_id": parent_file_id,
            "limit": 100,
            "type": "folder"
        }
        list_resp = requests.post(list_url, headers=headers, json=list_body, timeout=10)
        if list_resp.status_code == 200:
            items = list_resp.json().get("items", [])
            for item in items:
                if item.get("name") == folder_name and item.get("type") == "folder":
                    return item.get("file_id")
        
        # 如果没有，创建文件夹
        create_url = BASE_URL + "/adrive/v1.0/openFile/create"
        create_body = {
            "drive_id": drive_id,
            "parent_file_id": parent_file_id,
            "name": folder_name,
            "type": "folder",
            "check_name_mode": "auto_rename"
        }
        create_resp = requests.post(create_url, headers=headers, json=create_body, timeout=10)
        if create_resp.status_code == 200:
            create_data = create_resp.json()
            return create_data.get("file_id")
        else:
            return None
    except Exception as e:
        print("确保文件夹 {} 存在时出错: {}".format(folder_name, e))
        return None

def get_backup_folder_name(backup_main):
    """从配置中获取备份文件夹名称"""
    try:
        folder_name = backup_main._get_config_value('backup_folder', 'BTBackup')
        return folder_name if folder_name else 'BTBackup'
    except:
        return 'BTBackup'

def ensure_backup_folder(backup_main):
    """
    确保云盘中存在备份根目录，返回其 file_id
    在授权目录（folder_id）下创建备份文件夹（名称从配置读取）
    """
    backup_folder_name = get_backup_folder_name(backup_main)
    print("使用备份文件夹名称: {}".format(backup_folder_name))
    import json
    token_file = os.path.join(plugin_path, "token.json")
    if not os.path.exists(token_file):
        print("无法获取 token 信息，请先登录")
        return None
    
    try:
        with open(token_file, "r", encoding="utf-8") as f:
            token_info = json.load(f)
    except Exception as e:
        print("读取 token 文件失败: {}".format(e))
        return None
    
    if not token_info:
        print("无法获取 token 信息，请先登录")
        return None
    
    token_info, drive_err = backup_main._ensure_drive_info(token_info)
    if not token_info or (drive_err and not drive_err.get("status")):
        print("获取 drive 信息失败: {}".format(drive_err.get("msg") if drive_err else "未知错误"))
        return None

    access_token = token_info.get("access_token")
    if not access_token:
        print("token 中缺少 access_token")
        return None

    drive_id = token_info.get("effective_drive_id") or token_info.get("default_drive_id")
    # 优先使用授权的 folder_id，如果没有则使用 root
    parent_file_id = token_info.get("folder_id") or "root"
    
    print("使用 drive_id: {}, parent_file_id: {}".format(drive_id, parent_file_id))

    try:
        import requests
        BASE_URL = "https://openapi.alipan.com"
        headers = {
            "Authorization": "Bearer {}".format(access_token),
            "Content-Type": "application/json;charset=utf-8"
        }

        # 先列出当前目录，看是否已有备份文件夹
        list_url = BASE_URL + "/adrive/v1.0/openFile/list"
        list_body = {
            "drive_id": drive_id,
            "parent_file_id": parent_file_id,
            "limit": 100,
            "type": "folder"
        }
        list_resp = requests.post(list_url, headers=headers, json=list_body, timeout=10)
        if list_resp.status_code == 200:
            items = list_resp.json().get("items", [])
            for item in items:
                if item.get("name") == backup_folder_name and item.get("type") == "folder":
                    print("找到已存在的备份目录: {}".format(backup_folder_name))
                    return item.get("file_id")
        else:
            print("列出目录失败，HTTP 状态码: {}".format(list_resp.status_code))

        # 如果没有，创建备份文件夹
        print("正在创建备份目录: {}".format(backup_folder_name))
        create_url = BASE_URL + "/adrive/v1.0/openFile/create"
        create_body = {
            "drive_id": drive_id,
            "parent_file_id": parent_file_id,
            "name": backup_folder_name,
            "type": "folder",
            "check_name_mode": "auto_rename"
        }
        create_resp = requests.post(create_url, headers=headers, json=create_body, timeout=10)
        if create_resp.status_code == 200:
            create_data = create_resp.json()
            folder_id = create_data.get("file_id")
            print("成功创建备份目录，file_id: {}".format(folder_id))
            return folder_id
        else:
            err_msg = ""
            try:
                err_json = create_resp.json()
                err_code = err_json.get("code")
                err_message = err_json.get("message")
                err_msg = "code={}, message={}".format(err_code, err_message)
            except:
                err_msg = create_resp.text
            print("创建备份目录失败，HTTP 状态码: {}，响应: {}".format(create_resp.status_code, err_msg))
            return None
    except Exception as e:
        print("确保备份目录异常: {}".format(e))
        return None

def main():
    """主函数：备份所有启用的站点"""
    print("=" * 50)
    print("阿里云盘备份服务开始执行")
    print("=" * 50)

    ensure_runtime_dir()

    backup_main = aliyundrive_backup_main()

    # 获取备份根目录 file_id（在授权目录下创建 BTBackup 文件夹）
    print("正在确保备份根目录存在...")
    backup_root_id = ensure_backup_folder(backup_main)
    if not backup_root_id:
        print("无法获取或创建备份根目录，退出")
        print("提示：请确保已登录阿里云盘，并且授权目录有写入权限")
        return
    
    print("备份根目录 file_id: {}".format(backup_root_id))
    
    # 获取配置信息
    ext_map_str = backup_main._get_config_value('ext_map', '')
    exclude_dirs_str = backup_main._get_config_value('exclude_dirs', '')
    site_keep = int(backup_main._get_config_value('site_keep', '10'))
    db_keep = int(backup_main._get_config_value('db_keep', '10'))
    
    # 解析文件过滤配置
    ext_map = []
    if ext_map_str:
        ext_map = [line.strip() for line in ext_map_str.split('\n') if line.strip()]
    
    exclude_dirs = []
    if exclude_dirs_str:
        exclude_dirs = [line.strip() for line in exclude_dirs_str.split('\n') if line.strip()]
    
    print("文件过滤配置 - 排除扩展名: {}, 排除目录: {}".format(ext_map, exclude_dirs))
    print("备份数量限制 - 站点: {}, 数据库: {}".format(site_keep if site_keep > 0 else '不限制', db_keep if db_keep > 0 else '不限制'))
    
    # 获取 token 信息，用于后续创建子目录
    import json
    token_file = os.path.join(plugin_path, "token.json")
    token_info = None
    drive_id = None
    access_token = None
    if os.path.exists(token_file):
        try:
            with open(token_file, "r", encoding="utf-8") as f:
                token_info = json.load(f)
            if token_info:
                token_info, _ = backup_main._ensure_drive_info(token_info)
                if token_info:
                    drive_id = token_info.get("effective_drive_id") or token_info.get("default_drive_id")
                    access_token = token_info.get("access_token")
        except Exception as e:
            print("读取 token 信息失败: {}".format(e))

    # 读取已启用备份的站点
    try:
        webs = backup_main.Db('web').select()
    except Exception as e:
        print("读取站点配置失败: {}".format(e))
        webs = []

    if webs:
        print("找到 {} 个需要备份的站点".format(len(webs)))

    # 遍历每个站点进行备份
    for web in webs:
        site_name = web.get('name', 'unknown')
        site_path = web.get('path', '')
        print("\n" + "=" * 50)
        print("开始备份站点: {}".format(site_name))
        print("站点路径: {}".format(site_path))

        if not os.path.exists(site_path):
            print("站点路径不存在，跳过")
            backup_main.add_log("备份网站", "站点 {} 路径不存在，跳过".format(site_name))
            continue

        try:
            # 压缩站点目录
            date_str = backup_main.get_time("%Y%m%d%H%M%S")
            zip_name = "{}_web_{}.zip".format(site_name, date_str)
            zip_path = os.path.join(runtime_path, zip_name)

            # 规范化压缩文件路径
            zip_path = os.path.abspath(zip_path)
            print("正在压缩: {}".format(zip_name))
            print("压缩文件路径: {}".format(zip_path))
            # 应用文件过滤配置
            is_zip = backup_main.Zip(site_path, zip_path, ext_map=ext_map, exclude_dirs=exclude_dirs)
            if not is_zip:
                print("压缩失败，请检查站点目录权限和磁盘空间")
                backup_main.add_log("备份网站", "站点 {} 压缩失败，请检查目录权限和磁盘空间".format(site_name))
                continue

            # 等待文件系统同步，最多重试 5 次
            file_exists = False
            for retry in range(5):
                if os.path.exists(zip_path):
                    # 再次确认文件大小不为 0
                    try:
                        file_size = os.path.getsize(zip_path)
                        if file_size > 0:
                            file_exists = True
                            break
                    except OSError:
                        pass
                if retry < 4:
                    time.sleep(0.5)  # 等待 0.5 秒后重试
            
            if not file_exists:
                print("压缩文件不存在或大小为 0，路径: {}".format(zip_path))
                print("请检查磁盘空间和文件系统权限")
                backup_main.add_log("备份网站", "站点 {} 压缩文件不存在或大小为 0".format(site_name))
                continue

            zip_size = os.path.getsize(zip_path)
            print("压缩完成，文件大小: {} MB".format(round(zip_size / 1024 / 1024, 2)))

            # 上传到云盘前再次确认文件存在
            if not os.path.exists(zip_path):
                print("错误：上传前检查发现压缩文件不存在: {}".format(zip_path))
                backup_main.add_log("备份网站", "站点 {} 上传前检查发现压缩文件不存在".format(site_name))
                continue
            
            # 确保站点备份子目录存在：BTBackup/site/网站名/
            site_folder_id = ensure_folder_in_drive(backup_main, backup_root_id, "site", drive_id, access_token)
            if not site_folder_id:
                print("无法创建 site 目录，使用根目录")
                site_folder_id = backup_root_id
            else:
                # 确保网站名目录存在
                site_name_folder_id = ensure_folder_in_drive(backup_main, site_folder_id, site_name, drive_id, access_token)
                if not site_name_folder_id:
                    print("无法创建网站目录 {}，使用 site 目录".format(site_name))
                    site_name_folder_id = site_folder_id
                else:
                    site_folder_id = site_name_folder_id
            
            # 上传到云盘
            print("正在上传到阿里云盘...")
            success, msg, result = backup_main.upload_file_to_drive(
                zip_path,
                parent_file_id=site_folder_id,
                file_name=zip_name
            )

            if success:
                print("上传成功: {}".format(zip_name))
                backup_main.add_log("备份网站", "成功上传站点 {} 到 {}".format(site_name, zip_name))
                
                # 清理旧备份（如果设置了数量限制）
                if site_keep > 0:
                    file_prefix = "{}_web_".format(site_name)
                    deleted_count, cleanup_msg = backup_main.cleanup_old_backups(
                        site_folder_id, site_keep, file_prefix, drive_id
                    )
                    if deleted_count > 0:
                        print("清理旧备份: {}".format(cleanup_msg))
                        backup_main.add_log("备份网站", "站点 {} 清理了 {} 个旧备份".format(site_name, deleted_count))
                
                # 上传成功后删除本地压缩文件
                try:
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
                        print("已删除本地压缩文件: {}".format(zip_name))
                except Exception as e:
                    print("删除本地压缩文件失败: {} (文件路径: {})".format(e, zip_path))
            else:
                print("上传失败: {}".format(msg))
                backup_main.add_log("备份网站", "站点 {} 上传失败: {}".format(site_name, msg))
                # 上传失败时保留文件，以便排查问题
                print("上传失败，保留本地压缩文件以便排查: {}".format(zip_path))

        except Exception as e:
            print("备份站点 {} 时发生异常: {}".format(site_name, e))
            backup_main.add_log("备份网站", "站点 {} 备份异常: {}".format(site_name, str(e)))

    # 读取已启用备份的数据库
    try:
        databases = backup_main.Db('database').select()
    except Exception as e:
        print("读取数据库配置失败: {}".format(e))
        databases = []

    if databases:
        print("\n" + "=" * 50)
        print("找到 {} 个需要备份的数据库".format(len(databases)))
        
        # 获取 MySQL 配置
        port_conf = backup_main.Db('conf').where('key=?', 'port').find()
        mysql_root_conf = backup_main.Db('conf').where('key=?', 'mysql_root').find()
        
        port = port_conf.get('val') if port_conf and isinstance(port_conf, dict) else '3306'
        mysql_root = mysql_root_conf.get('val') if mysql_root_conf and isinstance(mysql_root_conf, dict) else ''
        
        if not mysql_root:
            print("警告：未配置 MySQL root 密码，跳过数据库备份")
            print("提示：请在【数据库备份】页面配置 MySQL root 密码")
        else:
            # 遍历每个数据库进行备份
            for db in databases:
                db_name = db.get('name', 'unknown')
                print("\n" + "=" * 50)
                print("开始备份数据库: {}".format(db_name))
                
                try:
                    # 生成备份文件名
                    date_str = backup_main.get_time("%Y%m%d%H%M%S")
                    pure_name = "{}_db_{}".format(db_name, date_str)
                    sql_file = os.path.join(runtime_path, pure_name + ".sql")
                    zip_file = os.path.join(runtime_path, pure_name + ".zip")
                    
                    # 规范化路径
                    sql_file = os.path.abspath(sql_file)
                    zip_file = os.path.abspath(zip_file)
                    
                    print("正在导出数据库: {}".format(db_name))
                    # 执行 mysqldump
                    # 使用 --single-transaction 确保数据一致性
                    shell_cmd = "mysqldump -R -E --hex-blob --opt --single-transaction --force --socket=/tmp/mysql.sock --default-character-set=utf8mb4 -P{} -uroot -p{} {} > {} 2>&1".format(
                        port, mysql_root, db_name, sql_file
                    )
                    
                    dump_result = os.system(shell_cmd)
                    if dump_result != 0:
                        print("数据库导出失败，返回码: {}".format(dump_result))
                        backup_main.add_log("备份数据库", "数据库 {} 导出失败".format(db_name))
                        continue
                    
                    if not os.path.exists(sql_file) or os.path.getsize(sql_file) == 0:
                        print("数据库导出文件不存在或大小为 0")
                        backup_main.add_log("备份数据库", "数据库 {} 导出文件无效".format(db_name))
                        continue
                    
                    sql_size = os.path.getsize(sql_file)
                    print("数据库导出完成，文件大小: {} MB".format(round(sql_size / 1024 / 1024, 2)))
                    
                    # 压缩 SQL 文件
                    print("正在压缩数据库文件...")
                    import zipfile
                    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        zipf.write(sql_file, pure_name + '.sql')
                    
                    if not os.path.exists(zip_file):
                        print("压缩文件创建失败")
                        backup_main.add_log("备份数据库", "数据库 {} 压缩失败".format(db_name))
                        # 清理 SQL 文件
                        try:
                            os.remove(sql_file)
                        except:
                            pass
                        continue
                    
                    zip_size = os.path.getsize(zip_file)
                    print("压缩完成，文件大小: {} MB".format(round(zip_size / 1024 / 1024, 2)))
                    
                    # 确保数据库备份子目录存在：BTBackup/database/数据库名/
                    database_folder_id = ensure_folder_in_drive(backup_main, backup_root_id, "database", drive_id, access_token)
                    if not database_folder_id:
                        print("无法创建 database 目录，使用根目录")
                        database_folder_id = backup_root_id
                    else:
                        # 确保数据库名目录存在
                        db_name_folder_id = ensure_folder_in_drive(backup_main, database_folder_id, db_name, drive_id, access_token)
                        if not db_name_folder_id:
                            print("无法创建数据库目录 {}，使用 database 目录".format(db_name))
                            db_name_folder_id = database_folder_id
                        else:
                            database_folder_id = db_name_folder_id
                    
                    # 上传到云盘
                    zip_name = pure_name + ".zip"
                    print("正在上传到阿里云盘...")
                    success, msg, result = backup_main.upload_file_to_drive(
                        zip_file,
                        parent_file_id=database_folder_id,
                        file_name=zip_name
                    )
                    
                    if success:
                        print("上传成功: {}".format(zip_name))
                        backup_main.add_log("备份数据库", "成功上传数据库 {} 到 {}".format(db_name, zip_name))
                        
                        # 清理旧备份（如果设置了数量限制）
                        if db_keep > 0:
                            file_prefix = "{}_db_".format(db_name)
                            deleted_count, cleanup_msg = backup_main.cleanup_old_backups(
                                database_folder_id, db_keep, file_prefix, drive_id
                            )
                            if deleted_count > 0:
                                print("清理旧备份: {}".format(cleanup_msg))
                                backup_main.add_log("备份数据库", "数据库 {} 清理了 {} 个旧备份".format(db_name, deleted_count))
                    else:
                        print("上传失败: {}".format(msg))
                        backup_main.add_log("备份数据库", "数据库 {} 上传失败: {}".format(db_name, msg))
                    
                    # 清理本地文件
                    try:
                        if os.path.exists(sql_file):
                            os.remove(sql_file)
                        if success and os.path.exists(zip_file):
                            os.remove(zip_file)
                            print("已删除本地备份文件")
                    except Exception as e:
                        print("删除本地文件失败: {}".format(e))
                        
                except Exception as e:
                    print("备份数据库 {} 时发生异常: {}".format(db_name, e))
                    backup_main.add_log("备份数据库", "数据库 {} 备份异常: {}".format(db_name, str(e)))
    else:
        print("没有启用的数据库备份任务")

    print("\n" + "=" * 50)
    print("备份服务执行完成")
    print("=" * 50)

    cleanup_runtime_dir()

if __name__ == '__main__':
    main()
