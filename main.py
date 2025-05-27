# main.py
from pkg.plugin.context import register, handler, BasePlugin, APIHost, EventContext
from pkg.plugin.events import GroupMessageReceived, PersonMessageReceived
from pkg.platform.types import *
import os
import re
import json
import asyncio
import subprocess
from datetime import datetime, timedelta, timezone

# 创建UTC+8时区
china_tz = timezone(timedelta(hours=8))

@register(name="TaskManager", description="全功能定时任务插件", version="3.0", author="xiaoxin")
class TaskManagerPlugin(BasePlugin):
    def __init__(self, host: APIHost):
    super().__init__(host)
    try:
        # 使用宿主程序的标准数据目录
        plugin_base_dir = os.path.join(self.ap.host.data_dir, "plugins", "TaskManager")
        self.data_dir = os.path.join(plugin_base_dir, "data")
        self.tasks_file = os.path.join(plugin_base_dir, "tasks.json")
        
        # 确保目录存在
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 初始化任务列表
        self.tasks = []
        self.load_tasks()
    except Exception as e:
        self.ap.logger.error(f"初始化失败: {str(e)}")
        raise
    
    async def initialize(self):
        self.load_tasks()
        await self.restart_scheduler()
        self.ap.logger.info("插件初始化完成")

    async def restart_scheduler(self):
        if hasattr(self, "check_task"):
            self.check_task.cancel()
        self.check_task = asyncio.create_task(self.schedule_checker())

    def load_tasks(self):
        if os.path.exists(self.tasks_file):
            try:
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    self.tasks = json.load(f)
                self.ap.logger.info(f"成功加载 {len(self.tasks)} 个定时任务")
            except Exception as e:
                self.ap.logger.error(f"任务加载失败: {str(e)}")

    def save_tasks(self):
        with open(self.tasks_file, "w", encoding="utf-8") as f:
            json.dump(self.tasks, f, indent=2, ensure_ascii=False)

    async def schedule_checker(self):
        self.ap.logger.info("定时检查器启动")
        while True:
            await asyncio.sleep(30 - datetime.now().second % 30)
            now = datetime.now(china_tz)
            current_time = now.strftime("%H:%M")
            
            for task in self.tasks.copy():
                if task["time"] == current_time and self.should_trigger(task, now):
                    try:
                        output = await self.execute_script(task["script_name"])
                        await self.send_task_result(
                            task["target_type"],
                            task["target_id"],
                            output
                        )
                        task["last_run"] = now.isoformat()
                        self.save_tasks()
                    except Exception as e:
                        self.ap.logger.error(f"任务执行失败: {str(e)}")

    async def send_task_result(self, target_type, target_id, message):
        try:
            await self.host.send_active_message(
                adapter=self.host.get_platform_adapters()[0],
                target_type=target_type,
                target_id=str(target_id),
                message=MessageChain([Plain(message[:2000])])
            )
        except Exception as e:
            self.ap.logger.error(f"消息发送失败: {str(e)}")

    def should_trigger(self, task, now):
        last_run = datetime.fromisoformat(task["last_run"]) if task.get("last_run") else None
        if last_run and last_run > now:
            return False
        return not last_run or (now - last_run).total_seconds() >= 86400

    async def execute_script(self, script_name: str):
        """支持中文脚本文件名的执行方法"""
        script_path = os.path.join(self.data_dir, f"{script_name}.py")
        
        self.ap.logger.debug(f"正在检查脚本路径: {script_path}")
        if not os.path.exists(script_path):
            available_files = ", ".join(os.listdir(self.data_dir))
            self.ap.logger.error(f"可用脚本文件: {available_files}")
            raise FileNotFoundError(f"脚本文件不存在: {script_name}.py")

        try:
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                text=True,
                timeout=30,
                encoding="utf-8"
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"脚本执行失败: {result.stderr}")
            
            return result.stdout
        except subprocess.TimeoutExpired:
            raise TimeoutError("脚本执行超时（30秒）")
        except Exception as e:
            raise

    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        msg = str(ctx.event.message_chain).strip()
        parts = msg.split(maxsplit=3)
        
        try:
            # 处理 /执行 命令
            if len(parts) >= 2 and parts[0] == "/执行":
                script_name = parts[1]
                output = await self.execute_script(script_name)
                await ctx.reply(MessageChain([Plain(f"✅ 执行成功\n{output[:1500]}")]))
                ctx.prevent_default()

            # 处理 /定时 命令
            elif len(parts) >= 2 and parts[0] == "/定时":
                if parts[1] == "添加" and len(parts) == 4:
                    await self.add_task(ctx, parts[2], parts[3])
                    ctx.prevent_default()
                elif parts[1] == "删除" and len(parts) == 3:
                    await self.delete_task(parts[2])
                    ctx.prevent_default()
                elif parts[1] == "列出":
                    await self.list_tasks(ctx)
                    ctx.prevent_default()

        except Exception as e:
            await ctx.reply(MessageChain([Plain(f"❌ 错误: {str(e)}")]))
            ctx.prevent_default()

    async def add_task(self, ctx: EventContext, name: str, time_str: str):
        if not re.match(r"^([01]\d|2[0-3]):([0-5]\d)$", time_str):
            raise ValueError("时间格式应为 HH:MM")

        script_path = os.path.join(self.data_dir, f"{name}.py")
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"请先在data目录创建 {name}.py")

        self.tasks.append({
            "script_name": name,
            "time": time_str,
            "target_type": ctx.event.launcher_type,
            "target_id": ctx.event.launcher_id,
            "last_run": None,
            "created": datetime.now(china_tz).isoformat()
        })
        self.save_tasks()
        await ctx.reply(MessageChain([Plain(f"✅ 定时任务已创建\n名称: {name}\n时间: 每日 {time_str}")]))

    async def delete_task(self, name: str):
        original_count = len(self.tasks)
        self.tasks = [t for t in self.tasks if t["script_name"] != name]
        if len(self.tasks) == original_count:
            raise ValueError("未找到指定任务")
        self.save_tasks()

    async def list_tasks(self, ctx: EventContext):
        if not self.tasks:
            await ctx.reply(MessageChain([Plain("当前没有定时任务")]))
            return

        task_list = ["📅 当前定时任务列表"]
        for task in self.tasks:
            status = "✅ 已激活" if task.get("last_run") else "⏳ 待触发"
            task_list.append(
                f"名称: {task['script_name']}\n"
                f"时间: 每日 {task['time']}\n"
                f"状态: {status}\n"
                "━━━━━━━━━━━━━━"
            )
        await ctx.reply(MessageChain([Plain("\n".join(task_list))]))

    def __del__(self):
        if hasattr(self, "check_task"):
            self.check_task.cancel()
