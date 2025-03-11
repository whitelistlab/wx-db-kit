import json
import os
import sys
import traceback
import requests
from urllib.parse import urljoin

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from app.DataBase import msg_db, misc_db, close_db
from app.DataBase.merge import merge_databases, merge_MediaMSG_databases
from app.decrypt import get_wx_info, decrypt
from app.log import logger
from app.util import path

# 配置文件路径和API地址
INFO_FILE_PATH = "./info.json"
DB_DIR = "./app/DataBase/Msg"  # 修改数据库输出目录
SERVER_API_URL = "http://api.lc044.love/"

def get_info(version_list=None):
    try:
        if version_list is None:
            file_path = './app/decrypt/version_list.json'
            if not os.path.exists(file_path):
                file_path = os.path.join(os.path.dirname(__file__), 'app', 'decrypt', 'version_list.json')
            with open(file_path, "r", encoding="utf-8") as f:
                version_list = json.loads(f.read())

        result = get_wx_info.get_info(version_list)
        if result == -1:
            print("错误：请登录微信。")
        elif result == -2:
            print("错误：微信版本不匹配，请手动填写信息。")
        elif result == -3:
            print("错误：未找到 WeChatWin.dll 文件。")
        elif isinstance(result, str):
            version = result
            version_bias = get_bias_add(version)
            if version_bias.get(version):
                logger.info(f"从云端获取内存基址: {version_bias}")
                result = get_wx_info.get_info(version_bias)
            else:
                logger.info(f"从云端获取内存基址失败: {version}")
                result = [-2, version]
        else:
            return result
    except Exception as e:
        logger.error(traceback.format_exc())
        print(f"未知错误：{e}")
    return None

def get_bias_add(version):
    url = urljoin(SERVER_API_URL, 'wxBiasAddr')
    data = {'version': version}
    try:
        response = requests.get(url, json=data)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.error(f"请求失败：{e}")
    return {}

def decrypt_db(db_path, key):
    close_db()
    output_dir = DB_DIR
    os.makedirs(output_dir, exist_ok=True)
    tasks = []
    if os.path.exists(db_path):
        for root, dirs, files in os.walk(db_path):
            for file in files:
                if file.endswith('.db') and file != 'xInfo.db':
                    inpath = os.path.join(root, file)
                    output_path = os.path.join(output_dir, file)
                    # 如果目标文件已存在，先删除它
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    tasks.append([key, inpath, output_path])
                elif file.endswith('.db_SQLITE'):
                    try:
                        name, suffix = file.split('.')
                        inpath = os.path.join(root, file)
                        output_path = os.path.join(output_dir, f"{name}.db")
                        # 如果目标文件已存在，先删除它
                        if os.path.exists(output_path):
                            os.remove(output_path)
                        tasks.append([key, inpath, output_path])
                    except Exception as e:
                        logger.error(f"处理文件 {file} 时出错：{e}")
                        continue
    for task in tasks:
        if decrypt.decrypt(*task) == -1:
            print(f"解密失败：{task[1]}")
            return False
    return True

def merge_databases_wrapper():
    import shutil
    # 确保MSG0.db存在
    msg0_path = os.path.join(DB_DIR, 'MSG0.db')
    if not os.path.exists(msg0_path):
        print("错误：未找到基础消息数据库文件MSG0.db")
        return False

    # 合并MSG数据库
    target_database = os.path.join(DB_DIR, 'MSG.db')
    source_databases = [os.path.join(DB_DIR, f"MSG{i}.db") for i in range(1, 50)]
    source_databases = [db for db in source_databases if os.path.exists(db)]
    
    if os.path.exists(target_database):
        os.remove(target_database)
    shutil.copy2(msg0_path, target_database)
    merge_databases(source_databases, target_database)

    # 确保MediaMSG0.db存在
    mediamsg0_path = os.path.join(DB_DIR, 'MediaMSG0.db')
    if not os.path.exists(mediamsg0_path):
        print("错误：未找到基础媒体数据库文件MediaMSG0.db")
        return False

    # 合并MediaMSG数据库
    target_database = os.path.join(DB_DIR, 'MediaMSG.db')
    source_databases = [os.path.join(DB_DIR, f"MediaMSG{i}.db") for i in range(1, 50)]
    source_databases = [db for db in source_databases if os.path.exists(db)]

    if os.path.exists(target_database):
        os.remove(target_database)
    shutil.copy2(mediamsg0_path, target_database)
    merge_MediaMSG_databases(source_databases, target_database)
    return True

def main():
    print("欢迎使用微信聊天记录解密工具！")
    print("1. 获取微信信息")
    print("2. 开始解密")
    print("3. 退出")

    while True:
        choice = input("请选择操作：")
        if choice == "1":
            info = get_info()
            if info:
                print("获取到的微信信息：")
                print(json.dumps(info, ensure_ascii=False, indent=4))
        elif choice == "2":
            db_path = input("请输入微信数据库文件夹路径（包含Msg文件夹）：")
            key = input("请输入解密密钥：")
            if decrypt_db(db_path, key):
                print("解密成功！")
                merge_databases_wrapper()
                print("数据库合并完成。")
            else:
                print("解密失败，请检查路径和密钥是否正确。")
        elif choice == "3":
            print("退出程序。")
            break
        else:
            print("无效的选项，请重新输入。")


if __name__ == "__main__":
    # wxid = "wxid_l2lr318x5j3822"
    # filepath = "C:\\Users\\23991\\Documents\\WeChat Files\\wxid_l2lr318x5j3822"
    # key = "6f22022a5001404abd8d7ba8cf066300485f96f3c49e4233b0dc65702e3a6afe"
    # decrypt_db(filepath, key)
    # print("解密成功！")
    # merge_databases_wrapper()
    # print("数据库合并完成。")

    main()