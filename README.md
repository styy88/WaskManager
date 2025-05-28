<h1 align="center">🚀 AstrBot 定时任务插件 🚀</h1>

<p align="center">
<strong>为你的 AstrBot添加定时任务，打造全功能定时任务插件！</strong>
</p>
<p align="center">
<code>ZaskManager</code> 是一个为 <a href="https://github.com/Soulter/AstrBot">AstrBot</a> 聊天机器人框架量身定制的全功能定时任务插件。
它允许您的机器人运行各种自定义脚本，执行/定时运行发送消息，
让机器人能够基于准确、具体的私有信息进行对话，区别不同群聊或者不同人物的私聊。
</p>
---
## 🎮 快速开始：指令
/定时 添加 [脚本名] [时间] - 创建新任务
/定时 删除 [任务ID或名称] - 删除任务
/定时 列出 - 显示当前会话任务
/执行 [脚本名] - 立即执行脚本

使用要求：
1.在插件数据目录下创建任务脚本
2.脚本命名规则：
允许中文、字母、数字和下划线
示例：定时.py、remaining_days.py、report.py
3.脚本输出规范：
支持 Markdown 格式图片：![描述](图片URL)
其他文本内容将直接显示
