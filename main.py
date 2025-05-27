from pkg.plugin.context import register, handler, BasePlugin, APIHost, EventContext
from pkg.plugin.events import GroupNormalMessageReceived, PersonNormalMessageReceived
from pkg.platform.types import *
import os
import re
import json
import asyncio
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set

# 创建UTC+8时区
china_tz = timezone(timedelta(hours=8))

def generate_task_id(script_name: str, time_str: str) -> str:
    """生成唯一任务ID（保留冒号版本）"""
    return f"{script_name}_{time_str.replace(':', '-')}"

@register(name="WaskManager", description="全功能定时任务管理", version="3.2", author="xiaoxin")
class WaskManagerPlugin(BasePlugin):
    def __init__(self, host: APIHost):
        super().__init__(host)
        self.tasks: List[Dict] = []
        self.task_timers: Dict[str, asyncio.Task] = {}
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        self.tasks_file = os.path.join(os.path.dirname(__file__), "tasks.json")
        os.makedirs(self.data_dir, exist_ok=True)
    
    async def initialize(self):
        self.load_tasks()
        await self.restart_scheduler()
        self.ap.logger.info("插件初始化完成")

    async def restart_scheduler(self):
        """重启调度器"""
        for task_id, timer in self.task_timers.items():
            timer.cancel()
        self.task_timers.clear()
        self.schedule_checker_task = asyncio.create_task(self.schedule_checker())

    def load_tasks(self):
        """增强加载逻辑"""
        try:
            if os.path.exists(self.tasks_file):
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    self.tasks = json.load(f)
                # ID兼容处理
                for task in self.tasks:
                    if "task_id" not in task:
                        task["task_id"] = generate_task_id(task["script_name"], task["time"])
                self.ap.logger.info(f"Loaded {len(self.tasks)} tasks")
        except Exception as e:
            self.ap.logger.error(f"加载失败: {str(e)}")
            self.tasks = []

    def save_tasks(self):
        """持久化存储任务"""
        with open(self.tasks_file, "w", encoding="utf-8") as f:
            json.dump(self.tasks, f, indent=2, ensure_ascii=False)

    async def schedule_checker(self):
        """增强型定时检查器"""
        self.ap.logger.info("智能调度器启动")
        while True:
            try:
                now = datetime.now(china_tz)
                # 计算下一个整分钟时间
                next_check = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                await asyncio.sleep((next_check - now).total_seconds())
                
                current_time = next_check.strftime("%H:%M")
                self.ap.logger.debug(f"开始检查 {current_time} 的任务")
                
                # 检查所有任务
                for task in self.tasks:
                    task_id = task["task_id"]
                    
                    # 跳过已安排的任务
                    if task_id in self.task_timers:
                        continue
                        
                    # 时间匹配检查
                    if task["time"] == current_time:
                        # 频率检查（每日执行）
                        last_run = datetime.fromisoformat(task["last_run"]) if task.get("last_run") else None
                        if last_run and (next_check - last_run).total_seconds() < 86400:
                            continue
                            
                        # 创建定时器
                        self.task_timers[task_id] = asyncio.create_task(
                            self.execute_task(task),
                            name=f"TaskExecutor-{task_id}"
                        )

            except asyncio.CancelledError:
                self.ap.logger.info("调度器正常关闭")
                break
            except Exception as e:
                self.ap.logger.error(f"调度器异常: {str(e)}")
                await asyncio.sleep(60)  # 错误冷却

    async def execute_task(self, task: Dict):
        """执行单个任务"""
        task_id = task["task_id"]
        try:
            self.ap.logger.info(f"开始执行任务 {task_id}")
            output = await self.execute_script(task["script_name"])
            await self.send_task_result(
                task["target_type"],
                task["target_id"],
                output
            )
            # 更新最后执行时间
            task["last_run"] = datetime.now(china_tz).isoformat()
            self.save_tasks()
            
        except Exception as e:
            self.ap.logger.error(f"任务执行失败 [{task_id}]: {str(e)}")
        finally:
            # 清理定时器
            if task_id in self.task_timers:
                del self.task_timers[task_id]

    async def send_task_result(self, target_type, target_id, message):
        """发送执行结果"""
        try:
            await self.host.send_active_message(
                adapter=self.host.get_platform_adapters()[0],
                target_type=target_type,
                target_id=str(target_id),
                message=MessageChain([Plain(message[:2000])])
            )
        except Exception as e:
            self.ap.logger.error(f"消息发送失败: {str(e)}")

    async def execute_script(self, script_name: str):
        """增强型脚本执行"""
        script_path = os.path.join(self.data_dir, f"{script_name}.py")
        
        if not os.path.exists(script_path):
            available = ", ".join(f.replace('.py', '') for f in os.listdir(self.data_dir))
            raise FileNotFoundError(
                f"脚本不存在！可用脚本: {available or '无'}"
            )

        try:
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                text=True,
                timeout=30,
                encoding="utf-8",
                check=True
            )
            return result.stdout
        except subprocess.TimeoutExpired:
            raise TimeoutError("执行超时（30秒限制）")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"执行失败（代码{e.returncode}）: {e.stderr}")
        except Exception as e:
            raise RuntimeError(f"未知错误: {str(e)}")

@handler(GroupNormalMessageReceived)
@handler(PersonNormalMessageReceived)
async def message_handler(self, ctx: EventContext):
    """兼容不同框架版本的消息处理器"""
    try:
        # 获取消息链的正确方式（关键修改点）
        if hasattr(ctx.event, 'query'):
            msg_chain = ctx.event.query.message_chain  # 适配新版框架
        else:
            msg_chain = ctx.event.message_chain  # 旧版框架兼容
            
        msg = str(msg_chain).strip()
        
        # 严格命令过滤
        if not msg.startswith(('/定时', '/执行')):
            return
            
        parts = msg.split(maxsplit=3)
        
        # 处理命令逻辑...
        
        ctx.prevent_default()

    except Exception as e:
        await ctx.reply(MessageChain([Plain(f"❌ 系统错误: {str(e)}")]))
        ctx.prevent_default()
        raise
    async def delete_task(self, ctx: EventContext, identifier: str = None):
        """增强删除逻辑"""
        target_id = ctx.event.launcher_id
        deleted = []
        
        for task in self.tasks.copy():
            if task["target_id"] != target_id:
                continue
                
            # 支持两种匹配方式（关键修改点5）
            match_condition = (
                identifier == task["task_id"] or  # 完全匹配ID
                identifier == task["script_name"]  # 匹配脚本名称
            )
            
            if match_condition:
                deleted.append(task)
                if timer := self.task_timers.pop(task["task_id"], None):
                    timer.cancel()
                self.tasks.remove(task)
        
        if not deleted:
            raise ValueError(f"未找到任务: {identifier}")
        
        self.save_tasks()
        
        # 生成友好报告
        report = [f"✅ 已删除 {len(deleted)} 个任务:"]
        for task in deleted:
            report.append(
                f"· {task['script_name']} ({task['time']})\n"
                f"  ID: {task['task_id']}"
            )
        await ctx.reply(MessageChain([Plain("\n".join(report))]))

    async def list_tasks(self, ctx: EventContext):
        """优化列表显示"""
        target_id = ctx.event.launcher_id
        my_tasks = [t for t in self.tasks if t["target_id"] == target_id]
        
        if not my_tasks:
            await ctx.reply(MessageChain([Plain("当前没有定时任务")]))
            return
            
        report = ["📅 您的定时任务列表 (输入/定时 删除 [ID或名称] 来管理)"]
        for task in my_tasks:
            status = "✅ 运行中" if task["task_id"] in self.task_timers else "⏸ 待触发"
            last_run = datetime.fromisoformat(task["last_run"]).strftime("%m/%d %H:%M") if task["last_run"] else "尚未执行"
            report.append(
                f"🔹 {task['script_name']}\n"
                f"  每日时间：{task['time']}\n"
                f"  任务状态：{status}\n"
                f"  最后执行：{last_run}\n"
                f"  唯一标识：{task['task_id']}\n"
                "━━━━━━━━━━━━━━"
            )
        await ctx.reply(MessageChain([Plain("\n".join(report))]))

    def __del__(self):
        """增强资源清理"""
        if hasattr(self, "schedule_checker_task"):
            self.schedule_checker_task.cancel()
        for timer in self.task_timers.values():
            timer.cancel()
