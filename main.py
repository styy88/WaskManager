from pkg.plugin.context import register, handler, BasePlugin, APIHost, EventContext
from pkg.plugin.events import GroupMessageReceived
import os
import json
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pkg.platform.types import *

# 创建UTC+8时区对象
china_tz = timezone(timedelta(hours=8))

@register(name="TaskManager", 
          description="定时任务管理插件（支持即时执行和定时任务）", 
          version="1.0", 
          author="xiaoxin")
class AutoTaskPlugin(BasePlugin):
    def __init__(self, host: APIHost):
        super().__init__(host)
        self.host = host
        self.tasks = []
        self.lock = asyncio.Lock()  # 同步锁初始化
        
        # 加载任务
        self.load_tasks()  # ✅ 现在能正确调用
        
        # 启动定时器
        self.check_timer_task = asyncio.create_task(self.timer_loop())
    
    def load_tasks(self):  # ✅ 正确定义在类内
        """加载任务列表"""
        try:
            with open('tasks.json', 'r') as f:
                self.tasks = json.load(f)
        except FileNotFoundError:
            self.tasks = []
    
    async def timer_loop(self):  # ✅ 异步方法
        async with self.lock:  # ✅ 正确使用异步锁
            while True:
                await asyncio.sleep(1)
            async with self.lock:
                now = datetime.now(china_tz)
                current_time = now.strftime("%H:%M")
                
                for task in self.tasks:
                    if self._should_trigger(task, now):
                        await self._execute_script(task["script_name"])
                        task["last_run"] = now.isoformat()
                        self.save_tasks()

    def _should_trigger(self, task, now):
        """判断任务是否应该触发"""
        if task["scheduled_time"] != now.strftime("%H:%M"):
            return False
            
        last_run = datetime.fromisoformat(task["last_run"]) if task["last_run"] else None
        return not last_run or (now - last_run).days >= 1

    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        """消息处理入口"""
        msg = str(ctx.event.message_chain).strip()
        
        # 权限验证
        if not self._check_permission(ctx):
            return

        # 命令路由
        if msg.startswith("/剩余天数"):
            await self.run_script(ctx, "remaining_days")
        elif msg.startswith("/添加"):
            await self.add_task(ctx, msg)
        elif msg.startswith("/删除"):
            await self.delete_task(ctx, msg)
        elif msg.startswith("/任务列表"):
            await self.list_tasks(ctx)
    
    async def run_script(self, ctx, script_name):
        """即时执行脚本"""
        script_path = os.path.join(self.data_dir, f"{script_name}.py")
        
        if not self._validate_script(script_name):
            await ctx.reply("脚本名称无效")
            return
            
        if not os.path.exists(script_path):
            await ctx.reply(f"脚本 {script_name}.py 不存在")
            return

        try:
            result = subprocess.run(
                ['python', script_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            output = result.stdout if result.stdout else "执行成功（无输出）"
            await self._send_output(ctx, output)
            
        except subprocess.TimeoutExpired:
            await ctx.reply("脚本执行超时")
        except Exception as e:
            await ctx.reply(f"执行出错: {str(e)}")

    async def add_task(self, ctx, msg):
        """添加定时任务"""
        parts = msg.split(maxsplit=2)
        if len(parts) != 3:
            await ctx.reply("格式错误，正确格式：/添加 脚本名 时间（例: /添加 report 08:30）")
            return
            
        _, script_name, schedule_time = parts
        
        # 验证脚本名称
        if not self._validate_script(script_name):
            await ctx.reply("脚本名称只能包含字母、数字和下划线")
            return
            
        # 验证时间格式
        if not re.match(r"^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$", schedule_time):
            await ctx.reply("时间格式应为HH:MM（例: 08:30）")
            return
            
        # 检查脚本存在
        if not os.path.exists(os.path.join(self.data_dir, f"{script_name}.py")):
            await ctx.reply(f"脚本 {script_name}.py 不存在")
            return
            
        async with self.lock:
            # 检查重复任务
            if any(t["script_name"] == script_name for t in self.tasks):
                await ctx.reply("该脚本已有定时任务")
                return
                
            new_task = {
                "script_name": script_name,
                "scheduled_time": schedule_time,
                "last_run": None,
                "created_at": datetime.now(china_tz).isoformat()
            }
            
            self.tasks.append(new_task)
            self.save_tasks()
            await ctx.reply(f"已添加定时任务：{script_name} 每天 {schedule_time} 执行")

    async def delete_task(self, ctx, msg):
        """删除定时任务"""
        parts = msg.split(maxsplit=1)
        if len(parts) != 2:
            await ctx.reply("格式错误，正确格式：/删除 脚本名")
            return
            
        _, script_name = parts
        
        async with self.lock:
            initial_count = len(self.tasks)
            self.tasks = [t for t in self.tasks if t["script_name"] != script_name]
            
            if len(self.tasks) < initial_count:
                self.save_tasks()
                await ctx.reply(f"已删除 {script_name} 的定时任务")
            else:
                await ctx.reply("未找到对应任务")

    async def list_tasks(self, ctx):
        """列出所有任务"""
        if not self.tasks:
            await ctx.reply("当前没有定时任务")
            return
            
        task_list = []
        for i, task in enumerate(self.tasks, 1):
            last_run = "从未执行" 
            if task["last_run"]:
                last_run = datetime.fromisoformat(task["last_run"]).strftime("%m-%d %H:%M")
                
            task_list.append(
                f"{i}. {task['script_name']} - 每天 {task['scheduled_time']}\n"
                f"   最后执行: {last_run} 创建于: {task['created_at'][:10]}"
            )
            
        await ctx.reply("\n\n".join(task_list))

    def _validate_script(self, name):
        """验证脚本名称合法性"""
        return re.match(r"^[a-zA-Z0-9_]+$", name) is not None

    async def _send_output(self, ctx, output):
        """处理脚本输出为消息链"""
        image_links = re.findall(r'!\[.*?\]\((https?://\S+)\)', output)
        elements = []
        
        # 添加图片
        for url in image_links:
            elements.append(Image(url=url))
            
        # 添加剩余文本
        text = re.sub(r'!\[.*?\]\(https?://\S+\)', '', output).strip()
        if text:
            elements.append(Plain(text))
            
        await ctx.reply(MessageChain(elements))

    def load_tasks(self):
        """加载任务列表"""
        task_file = os.path.join(os.path.dirname(__file__), 'tasks.json')
        if os.path.exists(task_file):
            try:
                with open(task_file, 'r', encoding='utf-8') as f:
                    self.tasks = json.load(f)
                    # 转换时间格式
                    for task in self.tasks:
                        if task.get('last_run'):
                            task['last_run'] = datetime.fromisoformat(task['last_run']).isoformat()
            except Exception as e:
                print(f"加载任务失败: {str(e)}")

    def save_tasks(self):
        """保存任务列表"""
        task_file = os.path.join(os.path.dirname(__file__), 'tasks.json')
        with open(task_file, 'w', encoding='utf-8') as f:
            json.dump(self.tasks, f, ensure_ascii=False, indent=2)

    def _check_permission(self, ctx):
        """权限验证（示例逻辑）"""
        # 此处实现你自己的权限验证逻辑
        # 例如只允许管理员或特定群组操作
        return True

    async def on_unregister(self):
        """插件卸载时清理"""
        if self.check_timer_task:
            self.check_timer_task.cancel()
