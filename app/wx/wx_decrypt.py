import os

import psutil
import requests
from pymem import Pymem
import time

from app.decrypt import get_wx_info, decrypt
from app.DataBase.merge import merge_databases, merge_MediaMSG_databases
from app.DataBase import close_db
from app.decrypt.get_wx_info import Wechat
from app.util import path
import json

from app.wx.wechat_accinfo import WeChatUserInfo


def manual_get_keyAndPath(name='', mobile='', wxid=''):
    """
    手动获取微信信息
    Args:
        name: 微信昵称
        mobile: 手机号
        wxid: 微信号
    Returns:
        dict: 包含微信信息的字典，获取失败返回None
    """
    
    # 尝试自动匹配微信目录
    wx_base_dir = path.wx_path()
    wechat_info = WeChatUserInfo(wx_base_dir)
    user_infos = wechat_info.get_user_infos()
    
    # 遍历用户信息，查找匹配项
    matched_info = None
    for user_info in user_infos:
        if (wxid and user_info.id.lower() == wxid.lower()) or \
           (name and name in user_info.name) or \
           (mobile and mobile in user_info.mobile):
            matched_info = user_info
            break
    
    if not matched_info:
        return None
        
    # 获取用户目录路径
    user_path = os.path.join(wx_base_dir, matched_info.id)
    if not os.path.exists(os.path.join(user_path, 'Msg')):
        return None
        
    try:
        # 获取密钥
        wechat = Pymem("WeChat.exe")
        key = Wechat(wechat).GetInfo()
        
        if not key:
            return None
            
        # 返回匹配到的信息
        return {
            'wxid': matched_info.id,
            'name': matched_info.name,
            'mobile': matched_info.mobile,
            'key': key,
            'filePath': user_path,
            'update_time': time.time()
        }
    except Exception as e:
        print(f"获取密钥时出错：{str(e)}")
        return None

def save_wechat_info(info):
    """保存微信信息到文件
    Args:
        info: 包含微信信息的字典
    """
    if not info:
        return
        
    try:
        # 准备要保存的数据
        save_data = {
            'wxid': info['wxid'],
            'wx_dir': info['filePath'],
            'name': info['name'],
            'mobile': info['mobile'],
            'key': info['key']
        }
        
        # 保存到文件
        try:
            with open('./app/data/info.json', 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=4)
        except:
            # 如果默认路径失败，尝试保存到当前目录
            with open('./info.json', 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=4)
                
        print("微信信息已保存到文件")
    except Exception as e:
        print(f"保存微信信息时出错：{str(e)}")

def load_wechat_info():
    """从文件读取微信信息
    Returns:
        dict: 包含微信信息的字典，读取失败返回None
    """
    try:
        # 尝试从默认路径读取
        try:
            with open('./app/data/info.json', 'r', encoding='utf-8') as f:
                info = json.load(f)
        except:
            # 如果默认路径失败，尝试从当前目录读取
            with open('./info.json', 'r', encoding='utf-8') as f:
                info = json.load(f)
        
        # 转换为程序内部使用的格式
        return {
            'wxid': info['wxid'],
            'name': info['name'],
            'mobile': info['mobile'],
            'key': info['key'],
            'filePath': info['wx_dir']
        }
    except Exception as e:
        print(f"从文件读取微信信息失败：{str(e)}")
        return None

def get_wechat_info():
    """获取微信信息，包括密钥、wxid等  """
    # 优先尝试从文件读取信息
    info = load_wechat_info()
    if info:
        print("已从文件读取微信信息")
        return info
        
    try:
        # 读取version_list.json文件
        version_list_path = os.path.join('app', 'decrypt', 'version_list.json')
        with open(version_list_path, 'r') as f:
            VERSION_LIST = json.load(f)
        
        info = get_wx_info.get_info(VERSION_LIST)
        if isinstance(info, int):
            if info == -1:
                print("错误：请登录微信")
                return None
        elif isinstance(info, str):
            # 版本不支持 从服务器获取新的偏移地址
            print("版本不支持，从服务器获取新的偏移地址")
            version = info
            try:
                url = 'http://api.lc044.love/wxBiasAddr'
                data = {'version': version}
                res = requests.get(url, json=data)
                print(res.json())
                if res.status_code == 200:
                    version_bias = res.json()
                    if version_bias.get(version):
                        print(f"从云端获取内存基址:{version_bias}")
                        info = get_wx_info.get_info(version_bias)
                        return info[0]
            except Exception as e:
                print(f"获取新版本偏移地址失败：{str(e)}")
                # 如果仍然失败，提示手动输入
            print(f"错误：当前微信版本 {version} 不受支持")
            print("请手动输入以下信息:")
            name = input("请输入微信昵称(直接回车跳过): ").strip()
            mobile = input("请输入手机号(直接回车跳过): ").strip()
            wxid = input("请输入微信号(直接回车跳过): ").strip()
            info = manual_get_keyAndPath(name, mobile, wxid)
            # 尝试手动获取信息
            return info
        elif not isinstance(info, list) or len(info) == 0:
            # 自动获取失败，尝试手动输入
            print("自动获取信息失败，请手动输入以下信息:")
            name = input("请输入微信昵称(直接回车跳过): ").strip()
            mobile = input("请输入手机号(直接回车跳过): ").strip()
            wxid = input("请输入微信号(直接回车跳过): ").strip()

            info = manual_get_keyAndPath(name, mobile, wxid)
            # 尝试手动获取信息
            return info
            
        info_result = info[0]  # 获取第一个微信进程的信息
        save_wechat_info(info_result)  # 保存信息到文件
        return info_result
    except Exception as e:
        print(f"获取微信信息时出错：{str(e)}")
        return None

def decrypt_databases(wx_dir, key):
    """解密微信数据库文件
    Args:
        wx_dir: 微信文件目录（包含Msg文件夹的目录）
        key: 解密密钥
    """
    if not wx_dir or not key:
        print("错误：微信目录或密钥无效")
        return False

    db_dir = os.path.join(wx_dir, 'Msg')
    if not os.path.exists(db_dir):
        print(f"错误：找不到数据库目录 {db_dir}")
        return False

    output_dir = os.path.abspath("./app/DataBase/Msg")
    os.makedirs(output_dir, exist_ok=True)

    # 解密数据库文件
    tasks = []
    for root, _, files in os.walk(db_dir):
        for file in files:
            if file.endswith('.db'):
                if file == 'xInfo.db':
                    continue
                inpath = os.path.join(root, file)
                output_path = os.path.join(output_dir, file)
                tasks.append([key, inpath, output_path])
            else:
                try:
                    name, suffix = file.split('.')
                    if suffix.startswith('db_SQLITE'):
                        inpath = os.path.join(root, file)
                        output_path = os.path.join(output_dir, name + '.db')
                        tasks.append([key, inpath, output_path])
                except:
                    continue

    print(f"找到 {len(tasks)} 个数据库文件需要解密")
    success_count = 0
    for task in tasks:
        try:
            result = decrypt.decrypt(*task)
            if result[0]:
                success_count += 1
                print(f"成功解密：{os.path.basename(task[1])}")
            else:
                print(f"解密失败：{os.path.basename(task[1])} - {result[1]}")
        except Exception as e:
            print(f"解密出错：{os.path.basename(task[1])} - {str(e)}")

    print(f"\n解密完成：成功 {success_count}/{len(tasks)} 个文件")

    # 合并数据库
    try:
        close_db()
        # 合并MSG数据库
        target_database = os.path.join(output_dir, 'MSG.db')
        source_databases = [os.path.join(output_dir, f"MSG{i}.db") for i in range(1, 50)]
        if os.path.exists(target_database):
            os.remove(target_database)
        import shutil
        shutil.copy2(os.path.join(output_dir, 'MSG0.db'), target_database)
        merge_databases(source_databases, target_database)

        # 合并MediaMSG数据库
        target_database = os.path.join(output_dir, 'MediaMSG.db')
        source_databases = [os.path.join(output_dir, f"MediaMSG{i}.db") for i in range(1, 50)]
        if os.path.exists(target_database):
            os.remove(target_database)
        shutil.copy2(os.path.join(output_dir, 'MediaMSG0.db'), target_database)
        merge_MediaMSG_databases(source_databases, target_database)
        print("\n数据库合并完成")
        return True
    except Exception as e:
        print(f"\n合并数据库时出错：{str(e)}")
        return False

def main():
    # 项目是怎么验证filePath与key是不是匹配的
    print("开始获取微信信息...")
    info = get_wechat_info()
    if info:
        print("开始解密数据库...")
        decrypt_databases(info['filePath'], info['key'])


if __name__ == '__main__':
    main()