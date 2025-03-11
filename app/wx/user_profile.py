import re
from collections import Counter
from jieba import analyse
from datetime import datetime
import os
from pymongo import MongoClient

class LocalUserProfileAnalyzer:
    def __init__(self, mongodb_uri: str = "mongodb://localhost:27017/", database_name: str = "wechat_msg"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.msg_collection = self.db.messages
        self.contacts_collection = self.db.contacts
        self.tfidf = analyse.extract_tags
        
        # 预定义的标签模式
        self.personal_tag_patterns = {
            '性别': {
                '女': r'(女士|小姐|美女|姐姐|她)',
                '男': r'(先生|男士|帅哥|哥哥|他)'
            },
            '年龄段': {
                '学生': r'(学生|上学|考试|作业)',
                '青年': r'(工作|加班|职场|结婚)',
                '中年': r'(孩子|家庭|育儿)'
            },
            '消费能力': {
                '高': r'(大牌|奢侈品|专柜|贵|质量)',
                '中': r'(性价比|优惠|打折|促销)',
                '低': r'(便宜|实惜|价格|实惠)',
            }
        }
        
    def clean_message(self, message):
        """清理消息中的多余空白字符"""
        return message.strip() if message else ''

    def analyze_chat_patterns(self, nickname):
        """分析用户的聊天模式和兴趣偏好"""
        # 通过nickname查找对应的str_talker
        contact = self.contacts_collection.find_one({"nickname": nickname})
        if not contact:
            return {
                'keywords': [],
                'interest_tags': [],
                'interest_tags_by_category': {},
                'personal_tags': {},
                'chat_frequency': 0,
                'text_messages_count': 0,
                'private_messages_count': 0,
                'group_messages_count': 0
            }
        
        str_talker = contact['username']
        
        # 从MongoDB获取私聊消息记录
        private_messages = list(self.msg_collection.find(
            {"str_talker": str_talker, "type": 1},  # type=1 表示文本消息
            {"str_content": 1, "create_time": 1}
        ))
        
        # 从MongoDB获取群聊中该用户的消息记录
        group_messages = list(self.msg_collection.find(
            {"user_name": str_talker, "type": 1},  # 使用user_name字段直接查询
            {"str_content": 1, "create_time": 1}
        ))
        
        # 合并私聊和群聊消息
        messages = private_messages + group_messages
        
        if not messages:
            return {
                'keywords': [],
                'interest_tags': [],
                'interest_tags_by_category': {},
                'chat_frequency': 0,
                'text_messages_count': 0
            }
            
        # 提取所有文本消息并清理
        text_messages = [self.clean_message(msg['str_content']) for msg in messages]
        text_messages = [msg for msg in text_messages if msg]  # 移除空消息
        
        if not text_messages:
            return {
                'keywords': [],
                'interest_tags': [],
                'interest_tags_by_category': {},
                'chat_frequency': 0,
                'text_messages_count': 0
            }

        
        # 使用TF-IDF提取关键词，降低权重阈值，增加topK数量
        all_text = ' '.join(text_messages)
        keywords = self.tfidf(all_text, topK=50, withWeight=True, allowPOS=('ns', 'n', 'vn', 'v', 'nr', 'a', 'ad', 'an'))
        
        # 生成个人标签和兴趣偏好
        interest_tags = self._generate_interest_tags(keywords)
        interest_tags_by_category = self._analyze_by_category_tags(keywords)
        personal_tags = self._extract_personal_tags(text_messages)
        
        return {
            'keywords': keywords,
            'interest_tags': interest_tags,
            'interest_tags_by_category': interest_tags_by_category,
            'personal_tags': personal_tags,
            'chat_frequency': len(messages),
            'text_messages_count': len(text_messages),
            'private_messages_count': len(private_messages),
            'group_messages_count': len(group_messages)
        }
    
    def _generate_interest_tags(self, keywords):
        """根据关键词生成个人标签"""
        interest_tags = []
        
        # 根据关键词内容添加个人标签
        interest_keywords = {
            '笔记侠': ['笔记', '记录', '整理', '总结', '归纳', '学习', '知识', '分享','视频'],
            '生活黑客': ['效率', '工具', '方法', '技巧', '经验', '实用', '解决', '优化'],
            '持续精进': ['学习', '进步', '成长', '提升', '突破', '目标', '规划', '发展'],
            '了不起的我': ['成就', '成功', '突破', '优秀', '卓越', '杰出', '领先', '创新'],
            '沟通高手': ['沟通', '表达', '交流', '演讲', '说服', '谈判', '协调', '合作'],
            '易受启发体质': ['灵感', '创意', '想法', '启发', '思考', '思维', '联想', '创新'],
            '带团队的高手': ['团队', '领导', '管理', '协作', '组织', '指导', '带领', '激励'],
            '跨界高手': ['跨界', '多元', '融合', '整合', '创新', '突破', '探索', '尝试'],
            '模型收集爱好者': ['模型', '收藏', '分析', '研究', '整理', '系统', '方法论', '框架'],
            '夜猫子': ['熬夜', '夜晚', '深夜', '通宵', '夜生活', '夜间', '夜猫'],
            '沉迷学习': ['学习', '研究', '探索', '钻研', '专注', '深入', '沉浸', '痴迷'],
            '早起鸟': ['早起', '晨练', '早安', '朝气', '活力', '清晨', '晨间', '晨光'],
            '剧书爱好者': ['追剧', '看书', '阅读', '电视剧', '电影', '文学', '小说', '故事'],
            '时间的朋友': ['时间', '管理', '规划', '效率', '安排', '计划', '执行', '目标'],
            '终身学习者': ['学习', '进步', '成长', '知识', '技能', '提升', '发展', '探索'],
            '长期主义': ['长期', '坚持', '持续', '稳定', '规划', '积累', '沉淀', '发展'],
            '我是中人': ['平衡', '中庸', '适度', '调和', '协调', '稳健', '理性', '务实'],
            '躺平入局': ['放松', '休息', '享受', '生活', '平和', '自在', '随性', '自由','可爱'],
        }
        
        # 添加所有类别的标签
        for category, words in interest_keywords.items():
            if any(word.lower() in kw.lower() for kw, _ in keywords for word in words):
                interest_tags.append(category)
        
        # 根据关键词权重调整标签权重
        weighted_tags = []
        for tag in interest_tags:
            weight = sum(w for kw, w in keywords if any(word.lower() in kw.lower() for word in interest_keywords.get(tag, [])))
            weighted_tags.append((tag, weight if weight > 0 else 0.05))  # 降低默认权重
        
        # 按权重排序并选择前10个兴趣标签
        weighted_tags.sort(key=lambda x: x[1], reverse=True)
        final_interest_tags = [tag for tag, _ in weighted_tags[:10]]
        
        return list(set(final_interest_tags))
        
    def _extract_personal_tags(self, chat_records):
        """提取个人标签"""
        combined_text = ' '.join(chat_records)
        tags = {}
        
        for category, patterns in self.personal_tag_patterns.items():
            max_count = 0
            selected_tag = None
            
            for tag, pattern in patterns.items():
                count = len(re.findall(pattern, combined_text))
                if count > max_count:
                    max_count = count
                    selected_tag = tag
            
            tags[category] = selected_tag if max_count > 0 else '未知'
        
        return tags
        
    def _analyze_by_category_tags(self, keywords):
        """根据兴趣偏好标签分析关键词"""
        category_tags = []
        
        # 使用interest_tags_by_category进行分析
        interest_tags_by_category = {
            '职场': ['职场', '工作', '事业', '职业', '发展', '晋升', '能力', '专业','微信','春招'],
            '管理学': ['管理', '领导', '团队', '战略', '决策', '组织', '规划', '执行'],
            '商业': ['商业', '市场', '营销', '策略', '品牌', '运营', '创新', '价值'],
            '互联网': ['互联网', '技术', '数字', '在线', '平台', '网络', '应用', '软件'],
            '品牌营销': ['品牌', '营销', '推广', '广告', '传播', '市场', '销售', '客户'],
            '经济学': ['经济', '金融', '市场', '贸易', '投资', '宏观', '微观', '政策'],
            '心理学': ['心理', '行为', '认知', '情绪', '思维', '性格', '意识', '潜意识'],
            '政治学': ['政治', '制度', '政策', '国际', '关系', '治理', '民主', '法制'],
            '自我提升': ['成长', '进步', '学习', '提升', '发展', '规划', '目标', '能力'],
            '金融学': ['金融', '投资', '理财', '基金', '股票', '资产', '风险', '收益'],
            '医学与健康': ['医学', '健康', '养生', '保健', '医疗', '疾病', '预防', '治疗'],
            '历史': ['历史', '文化', '传统', '古代', '近代', '现代', '文明', '变迁'],
            '家庭亲子': ['家庭', '亲子', '教育', '成长', '关系', '沟通', '陪伴', '养育'],
            '艺术': ['艺术', '音乐', '绘画', '设计', '创作', '美学', '文化', '审美'],
            '社会学': ['社会', '群体', '文化', '现象', '关系', '结构', '变迁', '发展'],
            '哲学': ['哲学', '思想', '逻辑', '价值', '伦理', '认知', '本质', '意义'],
            '科技': ['科技', '技术', '创新', '发展', '研究', '应用', '进步', '未来'],
            '自然科学': ['科学', '研究', '实验', '理论', '发现', '创新', '探索', '规律'],
            '法律': ['法律', '法规', '制度', '权利', '义务', '规范', '司法', '执法'],
            '文学': ['文学', '写作', '阅读', '创作', '文化', '艺术', '表达', '思想']
        }
        
        # 计算每个类别的权重
        weighted_categories = []
        for category, words in interest_tags_by_category.items():
            weight = sum(w for kw, w in keywords if any(word.lower() in kw.lower() for word in words))
            if weight > 0:
                weighted_categories.append((category, weight))
        
        # 按权重排序并选择前10个类别标签
        weighted_categories.sort(key=lambda x: x[1], reverse=True)
        category_tags = [tag for tag, _ in weighted_categories[:10]]
        
        return category_tags

# 创建分析器实例
analyzer = LocalUserProfileAnalyzer()

# 分析聊天记录（使用昵称）
nickname = "清秋十二"
chat_patterns = analyzer.analyze_chat_patterns(nickname)

# 打印分析结果
print('\n聊天模式分析结果:')
print(f'个人标签: {chat_patterns["interest_tags"]}') 
print(f'兴趣偏好: {chat_patterns["interest_tags_by_category"]}') 
print(f'个人标签: {chat_patterns["personal_tags"]}')
print(f'聊天频率: {chat_patterns["chat_frequency"]} 条消息')