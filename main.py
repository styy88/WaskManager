from pkg.plugin.context import register, handler, BasePlugin, APIHost, EventContext
from pkg.plugin.events import GroupMessageReceived, PersonMessageReceived
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
    """生成唯一任务ID"""
    return f"{script_name}_{time_str.replace(':', '')}"

@register(name="WaskManagerPro", description="增强版定时任务管理", version="3.1", author="xiaoxin")
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
        # 取消所有现有定时器
        for task_id, timer in self.task_timers.items():
            timer.cancel()
        self.task_timers.clear()
        
        # 重新创建检查任务
        self.schedule_checker_task = asyncio.create_task(self.schedule_checker())

    def load_tasks(self):
        """加载存储的任务"""
        if os.path.exists(self.tasks_file):
            try:
                with open(self.tasks_file, "r", encoding="utf-8") as f:
                    raw_tasks = json.load(f)
                    # 兼容旧版本数据
                    self.tasks = [
                        {**task, "task_id": task.get("task_id") or generate_task_id(task["script_name"], task["time"])}
                        for task in raw_tasks
                    ]
                self.ap.logger.info(f"成功加载 {len(self.tasks)} 个定时任务")
            except Exception as e:
                self.ap.logger.error(f"任务加载失败: {str(e)}")
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

    @handler(GroupMessageReceived)
    @handler(PersonMessageReceived)
    async def message_handler(self, ctx: EventContext):
        msg = str(ctx.event.message_chain).strip()
        parts = msg.split(maxsplit=3)
        
        try:
            if parts[0] == "/定时":
                await self.handle_schedule_command(ctx, parts)
            elif parts[0] == "/执行":
                await self.handle_execute_command(ctx, parts)
                
        except Exception as e:
            await ctx.reply(MessageChain([Plain(f"❌ 错误: {str(e)}")]))
            ctx.prevent_default()

    async def handle_schedule_command(self, ctx: EventContext, parts: List[str]):
        """处理定时命令"""
        if parts[1] == "添加" and len(parts) == 4:
            await self.add_task(ctx, parts[2], parts[3])
        elif parts[1] == "删除":
            await self.delete_task(ctx, parts[2] if len(parts)>=3 else None)
        elif parts[1] == "列出":
            await self.list_tasks(ctx)
        else:
            raise ValueError("无效命令格式")

    async def handle_execute_command(self, ctx: EventContext, parts: List[str]):
        """处理立即执行命令"""
        if len(parts) < 2:
            raise ValueError("缺少脚本名称")
            
        output = await self.execute_script(parts[1])
        await ctx.reply(MessageChain([Plain(f"✅ 执行成功\n{output[:1500]}")]))
        ctx.prevent_default()

    async def add_task(self, ctx: EventContext, name: str, time_str: str):
        """添加新任务"""
        # 验证时间格式
        if not re.fullmatch(r"^(0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$", time_str):
            raise ValueError("时间格式应为 HH:MM (24小时制)")
            
        # 检查脚本存在性
        script_path = os.path.join(self.data_dir, f"{name}.py")
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"脚本不存在，请先创建 {name}.py")
            
        # 生成任务ID
        task_id = generate_task_id(name, time_str)
        
        # 防止重复添加
        if any(t["task_id"] == task_id and t["target_id"] == ctx.event.launcher_id for t in self.tasks):
            raise ValueError("相同任务已存在")
            
        # 添加任务
        self.tasks.append({
            "task_id": task_id,
            "script_name": name,
            "time": time_str,
            "target_type": ctx.event.launcher_type,
            "target_id": ctx.event.launcher_id,
            "last_run": None,
            "created": datetime.now(china_tz).isoformat()
        })
        self.save_tasks()
        
        await ctx.reply(MessageChain([Plain(
            f"✅ 定时任务创建成功\n"
            f"名称：{name}\n"
            f"时间：每日 {time_str}\n"
            f"ID：{task_id}"
        )]))

    async def delete_task(self, ctx: EventContext, identifier: str = None):
        """删除任务（支持ID或名称）"""
        target_id = ctx.event.launcher_id
        
        # 获取要删除的任务
        to_delete = []
        for task in self.tasks.copy():
            # 匹配当前会话的任务
            if task["target_id"] != target_id:
                continue
                
            # ID匹配或名称匹配
            if identifier in (task["task_id"], task["script_name"]):
                to_delete.append(task)
                
        # 执行删除
        if not to_delete:
            raise ValueError("未找到匹配任务")
            
        # 取消关联定时器
        for task in to_delete:
            if timer := self.task_timers.pop(task["task_id"], None):
                timer.cancel()
            self.tasks.remove(task)
            
        self.save_tasks()
        
        # 生成报告
        report = ["✅ 已删除以下任务："]
        for task in to_delete:
            report.append(
                f"· {task['script_name']} ({task['time']}) "
                f"[ID: {task['task_id']}]"
            )
            
        await ctx.reply(MessageChain([Plain("\n".join(report))]))

    async def list_tasks(self, ctx: EventContext):
        """列出当前会话的任务"""
        target_id = ctx.event.launcher_id
        my_tasks = [t for t in self.tasks if t["target_id"] == target_id]
        
        if not my_tasks:
            await ctx.reply(MessageChain([Plain("当前没有定时任务")]))
            return
            
        report = ["📅 您的定时任务列表"]
        for task in my_tasks:
            status = "✅ 已激活" if task["task_id"] in self.task_timers else "⏸ 等待中"
            last_run = datetime.fromisoformat(task["last_run"]).strftime("%m-%d %H:%M") if task["last_run"] else "从未执行"
            report.append(
                f"🔹 {task['script_name']}\n"
                f"  时间：每日 {task['time']}\n"
                f"  状态：{status}\n"
                f"  最后执行：{last_run}\n"
                f"  ID：{task['task_id']}\n"
                "━━━━━━━━━━━━"
            )
            
        await ctx.reply(MessageChain([Plain("\n".join(report))]))

    def __del__(self):
        """清理资源"""
        # 取消所有定时器
        for timer in self.task_timers.values():
            timer.cancel()
        # 取消调度器
        if hasattr(self, "schedule_checker_task"):
            self.schedule_checker_task.cancel()
