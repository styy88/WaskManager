from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.message_components import Plain, Image
from astrbot.core.utils.io import download_image_by_url
import os
import re
import json
import asyncio
import subprocess
from datetime import datetime, timedelta, timezone
from typing import List, Dict

# ---------- 核心修复：消息发送机制 ----------
china_tz = timezone(timedelta(hours=8))

@register("ZaskManager", "xiaoxin", "全功能定时任务插件", "3.5", "https://github.com/styy88/ZaskManager")
class ZaskManager(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.context = context
        self._init_paths()
        self.tasks = self._safe_load_tasks()
        self.schedule_checker_task = asyncio.create_task(self._schedule_loop())

    # ---------- 消息发送统一接口 ----------
    async def _send_message(self, task: Dict, components: list) -> bool:
        """统一消息发送接口（适配v4.3.1+）"""
        try:
            # 构造标准会话ID
            session_id = f"{task['platform']}:{task['receiver_type']}:{task['origin_id'].split('!')[-1]}"
            
            # 处理图片下载
            processed = []
            for comp in components:
                if isinstance(comp, Image) and comp.file.startswith("http"):
                    local_path = await download_image_by_url(comp.file)
                    processed.append(Image(file=f"file:///{local_path}"))
                else:
                    processed.append(comp)
            
            # 使用框架标准接口
            await self.context.send_message(
                session_id=session_id,
                message_chain=processed
            )
            return True
        except Exception as e:
            logger.error(f"消息发送失败: {str(e)}")
            return False

    # ---------- 定时任务核心逻辑 ----------
    async def _schedule_loop(self):
        """稳健的定时循环逻辑"""
        logger.info("定时服务启动")
        while True:
            try:
                now = datetime.now(china_tz)
                next_check = (30 - (now.second % 30)) % 30
                await asyncio.sleep(next_check)
                
                current_time = datetime.now(china_tz).strftime("%H:%M")
                logger.debug(f"定时检查: {current_time}")
                
                for task in self.tasks.copy():
                    if task["time"] == current_time and self._should_trigger(task):
                        logger.info(f"执行任务: {task['task_id']}")
                        await self._execute_and_notify(task)
                        
            except asyncio.CancelledError:
                logger.info("定时服务终止")
                break
            except Exception as e:
                logger.error(f"定时循环异常: {str(e)}")
                await asyncio.sleep(10)

    async def _execute_and_notify(self, task: Dict):
        """带重试的任务执行逻辑"""
        max_retries = 2
        for attempt in range(1, max_retries+1):
            try:
                output = await self._run_script(task["script_name"])
                success = await self._send_message(task, [Plain(text=output[:2000])])
                if success:
                    task["last_run"] = datetime.now(china_tz).isoformat()
                    self._save_tasks()
                    break
                else:
                    logger.warning(f"消息发送失败，重试 {attempt}/{max_retries}")
            except Exception as e:
                error_msg = f"尝试 {attempt} 失败: {str(e)}"
                logger.error(error_msg)
                if attempt == max_retries:
                    await self._send_message(task, [Plain(text=f"❌ 任务最终失败: {error_msg[:500]}")])

    # ---------- 数据持久化 ----------
    def _safe_load_tasks(self) -> List[Dict]:
        """带数据迁移的加载逻辑"""
        try:
            if not os.path.exists(self.tasks_file):
                return []
                
            with open(self.tasks_file, 'r', encoding='utf-8') as f:
                tasks = json.load(f)
                
            # 数据格式迁移
            migrated = []
            for t in tasks:
                if 'origin_id' not in t:
                    t['origin_id'] = f"{t.get('platform','unknown')}!{t['receiver_type']}!{t.get('receiver','')}"
                migrated.append(t)
                
            return [t for t in migrated if self._validate_task(t)]
            
        except Exception as e:
            logger.error(f"任务加载失败: {str(e)}")
            return []

    def _save_tasks(self):
        """原子化保存"""
        try:
            tmp_file = self.tasks_file + '.tmp'
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(self.tasks, f, indent=2, ensure_ascii=False)
            os.replace(tmp_file, self.tasks_file)
        except Exception as e:
            logger.error(f"任务保存失败: {str(e)}")

    # ---------- 命令处理 ----------
    @filter.command("定时")
    async def handle_command(self, event: AstrMessageEvent):
        """命令路由"""
        try:
            parts = event.message_str.strip().split()
            if len(parts) < 2:
                return await self._show_help(event)

            cmd = parts[1].lower()
            if cmd == '添加' and len(parts) >=4:
                await self._add_task(event, parts[2], parts[3])
            elif cmd == '删除' and len(parts)>=3:
                await self._delete_task(event, parts[2])
            elif cmd == '列出':
                await self._list_tasks(event)
            else:
                await self._show_help(event)
                
        except Exception as e:
            await event.reply(f"❌ 命令处理失败: {str(e)}")

    # ---------- 辅助方法 ----------
    def _init_paths(self):
        """路径初始化"""
        self.plugin_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "plugin_data", "ZaskManager")
        )
        self.tasks_file = os.path.join(self.plugin_root, "tasks.json")
        os.makedirs(self.plugin_root, exist_ok=True)

    def _validate_task(self, task: Dict) -> bool:
        """数据校验"""
        return all(key in task for key in ['script_name', 'time', 'origin_id', 'platform'])

    def _should_trigger(self, task: Dict) -> bool:
        """触发条件检查"""
        last_run = datetime.fromisoformat(task['last_run']).astimezone(china_tz) if task.get('last_run') else None
        return not last_run or (datetime.now(china_tz) - last_run).days >=1

    async def _run_script(self, name: str) -> str:
        """安全的脚本执行"""
        path = os.path.join(self.plugin_root, f"{name}.py")
        if not os.path.exists(path):
            raise FileNotFoundError(f"脚本 {name} 不存在")
            
        proc = await asyncio.create_subprocess_exec(
            'python', path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"执行失败({proc.returncode}): {stderr.decode()}")
            
        return stdout.decode('utf-8')

    # ---------- 业务逻辑 ----------
    async def _add_task(self, event: AstrMessageEvent, name: str, time: str):
        """添加任务（带完整校验）"""
        # 时间格式校验
        if not re.match(r"^([01]\d|2[0-3]):([0-5]\d)$", time):
            raise ValueError("时间格式应为HH:MM")
            
        # 脚本存在性检查
        if not os.path.exists(os.path.join(self.plugin_root, f"{name}.py")):
            available = [f[:-3] for f in os.listdir(self.plugin_root) if f.endswith('.py')]
            raise FileNotFoundError(f"可用脚本: {', '.join(available) or '无'}")
            
        # 构造任务数据
        new_task = {
            "script_name": name,
            "time": time,
            "origin_id": event.unified_msg_origin,
            "platform": event.get_platform_name().lower(),
            "receiver_type": "group" if event.get_group_id() else "private",
            "last_run": None,
            "created": datetime.now(china_tz).isoformat(),
            "task_id": f"{name}_{time.replace(':','')}_{hash(event.unified_msg_origin)}"
        }
        
        # 重复性检查
        if any(t['task_id'] == new_task['task_id'] for t in self.tasks):
            raise ValueError(f"任务已存在: {new_task['task_id']}")
            
        self.tasks.append(new_task)
        self._save_tasks()
        await event.reply(
            f"✅ 任务创建成功\n"
            f"名称: {name}\n"
            f"时间: 每日 {time}\n"
            f"ID: {new_task['task_id']}"
        )

    async def _delete_task(self, event: AstrMessageEvent, identifier: str):
        """删除当前会话的任务"""
        current_origin = event.unified_msg_origin
        platform = event.get_platform_name().lower()
        
        current_tasks = [
            t for t in self.tasks 
            if t["receiver_origin"] == current_origin
            and t["platform"] == platform
        ]
        
        if not current_tasks:
            yield event.plain_result("当前会话没有定时任务")
            return
            
        deleted = []
        for task in current_tasks.copy():
            if identifier.lower() in (task["task_id"].lower(), task["script_name"].lower()):
                self.tasks.remove(task)
                deleted.append(task)
                
        if not deleted:
            available_ids = "\n".join([f"· {t['task_id']}" for t in current_tasks])
            raise ValueError(f"未找到匹配任务，当前可用ID：\n{available_ids}")
            
        self._save_tasks()
        
        report = ["✅ 已删除以下任务："]
        for task in deleted:
            report.append(
                f"▫️ {task['script_name']} ({task['time']})\n"
                f"  平台：{task['platform']}\n"
                f"  任务ID：{task['task_id']}\n"
                "━━━━━━━━━━━━━━━━"
            )
        yield event.plain_result("\n".join(report))

    async def _list_tasks(self, event: AstrMessageEvent):
        """列出当前会话任务"""
        current_origin = event.unified_msg_origin
        platform = event.get_platform_name().lower()
        
        current_tasks = [
            t for t in self.tasks 
            if t["receiver_origin"] == current_origin
            and t["platform"] == platform
        ]
        
        if not current_tasks:
            yield event.plain_result("当前会话没有定时任务")
            return
            
        task_list = [
            "📅 当前会话定时任务列表",
            f"会话类型：{'群聊' if receiver_type == 'group' else '私聊'}",
            f"平台类型：{platform}",
            f"会话ID：{receiver}",
            "━━━━━━━━━━━━━━━━"
        ]
        
        for index, task in enumerate(current_tasks, 1):
            status = "✅ 已激活" if task.get("last_run") else "⏳ 待触发"
            last_run = datetime.fromisoformat(task["last_run"]).strftime("%m/%d %H:%M") if task.get("last_run") else "尚未执行"
            
            task_list.extend([
                f"▪️ 任务 {index}",
                f"名称：{task['script_name']}",
                f"时间：每日 {task['time']}",
                f"状态：{status}",
                f"最后执行：{last_run}",
                f"任务ID：{task['task_id']}",
                "━━━━━━━━━━━━━━━━"
            ])
            
        yield event.plain_result("\n".join(task_list))

    @filter.command("执行")
    async def execute_command(self, event: AstrMessageEvent):
        """处理立即执行命令"""
        try:
            parts = event.message_str.split(maxsplit=1)
            if len(parts) < 2:
                raise ValueError("格式应为：/执行 [脚本名]")

            script_name = parts[1].strip()
            output = await self._execute_script(script_name)
            yield event.plain_result(f"✅ 执行成功\n{output[:1500]}")
        except Exception as e:
            event.stop_event()
            yield event.plain_result(f"❌ 执行错误: {str(e)}")

    async def _show_help(self, event: AstrMessageEvent):
        """显示帮助信息"""
        help_msg = """
📘 定时任务插件使用指南（v3.5+）

【核心功能】
✅ 跨平台支持：微信/QQ/Telegram
✅ 群聊 & 私聊消息
✅ 脚本执行结果自动发送

【命令列表】
/定时 添加 [脚本名] [时间] - 创建新任务
/定时 删除 [任务ID或名称] - 删除任务
/定时 列出 - 显示当前会话任务
/执行 [脚本名] - 立即执行脚本

【示例】
/定时 添加 数据备份 08:30 -> 创建每日备份
/定时 删除 备份任务_0830 -> 移除任务
/执行 生成报表 -> 立即运行脚本

【注意事项】
1. 脚本需放置在 plugin_data/ZaskManager 目录
2. 时间格式为 24小时制（如 14:00）
3. 任务ID在添加成功后显示
        """.strip()
        yield event.plain_result(help_msg)

    async def terminate(self):
        """插件卸载时停止所有任务"""
        if hasattr(self, "schedule_checker_task"):
            self.schedule_checker_task.cancel()
        logger.info("插件已安全卸载")
