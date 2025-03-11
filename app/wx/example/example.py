from user_analyzer import UserAnalyzer

def main():
    # 创建示例聊天记录
    chat_records = [
        "您好，我想买一款适合上班用的包包，预算2000左右",
        "我比较喜欢简约的风格，之前在专柜看过一些",
        "平时工作比较忙，希望包包耐用一些",
        "我是一名女士，经常需要带笔记本电脑",
        "周末也喜欢去健身，如果包包能放运动服就更好了"
    ]
    
    # 初始化分析器
    analyzer = UserAnalyzer()
    
    # 生成用户画像
    user_profile = analyzer.generate_user_profile(chat_records)
    
    # 打印结果
    print("\n=== 用户画像分析结果 ===")
    print("\n个人标签：")
    for category, value in user_profile["个人标签"].items():
        print(f"{category}: {value}")
    
    print("\n兴趣偏好：")
    for category, score in user_profile["兴趣偏好"].items():
        if score > 0:
            print(f"{category}: {score:.2%}")

if __name__ == "__main__":
    main() 