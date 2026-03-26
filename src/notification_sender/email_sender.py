# -*- coding: utf-8 -*-
"""
Email 发送提醒服务

职责：
1. 通过 SMTP 发送 Email 消息
"""
import logging
import re
from io import BytesIO
from typing import Optional, List
from datetime import datetime
from html import escape
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.header import Header
from email.utils import formataddr
import smtplib

from src.config import Config
from src.formatters import markdown_to_html_document


logger = logging.getLogger(__name__)


# SMTP 服务器配置（自动识别）
SMTP_CONFIGS = {
    # QQ邮箱
    "qq.com": {"server": "smtp.qq.com", "port": 465, "ssl": True},
    "foxmail.com": {"server": "smtp.qq.com", "port": 465, "ssl": True},
    # 网易邮箱
    "163.com": {"server": "smtp.163.com", "port": 465, "ssl": True},
    "126.com": {"server": "smtp.126.com", "port": 465, "ssl": True},
    # Gmail
    "gmail.com": {"server": "smtp.gmail.com", "port": 587, "ssl": False},
    # Outlook
    "outlook.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    "hotmail.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    "live.com": {"server": "smtp-mail.outlook.com", "port": 587, "ssl": False},
    # 新浪
    "sina.com": {"server": "smtp.sina.com", "port": 465, "ssl": True},
    # 搜狐
    "sohu.com": {"server": "smtp.sohu.com", "port": 465, "ssl": True},
    # 阿里云
    "aliyun.com": {"server": "smtp.aliyun.com", "port": 465, "ssl": True},
    # 139邮箱
    "139.com": {"server": "smtp.139.com", "port": 465, "ssl": True},
}


class EmailSender:
    
    def __init__(self, config: Config):
        """
        初始化 Email 配置

        Args:
            config: 配置对象
        """
        self._email_config = {
            'sender': config.email_sender,
            'sender_name': getattr(config, 'email_sender_name', 'daily_stock_analysis股票分析助手'),
            'password': config.email_password,
            'receivers': config.email_receivers or ([config.email_sender] if config.email_sender else []),
            'attachment_format': getattr(config, 'email_attachment_format', 'none'),
        }
        self._stock_email_groups = getattr(config, 'stock_email_groups', None) or []
        
    def _is_email_configured(self) -> bool:
        """检查邮件配置是否完整（只需邮箱和授权码）"""
        return bool(self._email_config['sender'] and self._email_config['password'])
    
    def get_receivers_for_stocks(self, stock_codes: List[str]) -> List[str]:
        """
        Look up email receivers for given stock codes based on stock_email_groups.
        Returns union of receivers for all matching groups; falls back to default if none match.
        """
        if not stock_codes or not self._stock_email_groups:
            return self._email_config['receivers']
        seen: set = set()
        result: List[str] = []
        for stocks, emails in self._stock_email_groups:
            for code in stock_codes:
                if code in stocks:
                    for e in emails:
                        if e not in seen:
                            seen.add(e)
                            result.append(e)
                    break
        return result if result else self._email_config['receivers']

    def get_all_email_receivers(self) -> List[str]:
        """
        Return union of all configured email receivers (all groups + default).
        Used for market review which should go to everyone.
        """
        seen: set = set()
        result: List[str] = []
        for _, emails in self._stock_email_groups:
            for e in emails:
                if e not in seen:
                    seen.add(e)
                    result.append(e)
        for e in self._email_config['receivers']:
            if e not in seen:
                seen.add(e)
                result.append(e)
        return result

    def _format_sender_address(self, sender: str) -> str:
        """Encode display name safely so non-ASCII sender names work across SMTP providers."""
        sender_name = self._email_config.get('sender_name') or '股票分析助手'
        return formataddr((str(Header(str(sender_name), 'utf-8')), sender))

    def _should_attach_pdf(self) -> bool:
        return (self._email_config.get('attachment_format') or 'none') == 'pdf'

    @staticmethod
    def _normalize_text_for_pdf(content: str) -> str:
        normalized_chars: List[str] = []
        for ch in (content or "").replace('\r\n', '\n').replace('\r', '\n'):
            if ch in {'\n', '\t'}:
                normalized_chars.append(ch)
                continue
            if ord(ch) < 32:
                continue
            if ord(ch) > 0xFFFF:
                normalized_chars.append('?')
                continue
            normalized_chars.append(ch)
        return ''.join(normalized_chars).replace('\t', '    ')

    @staticmethod
    def _strip_markdown_for_pdf(text: str) -> str:
        cleaned = text or ""
        cleaned = re.sub(r"`([^`]*)`", r"\1", cleaned)
        cleaned = re.sub(r"\*\*([^*]+)\*\*", r"\1", cleaned)
        cleaned = re.sub(r"__([^_]+)__", r"\1", cleaned)
        cleaned = re.sub(r"\*([^*]+)\*", r"\1", cleaned)
        cleaned = re.sub(r"_([^_]+)_", r"\1", cleaned)
        cleaned = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", cleaned)
        cleaned = re.sub(
            "["
            "\U0001F300-\U0001F6FF"
            "\U0001F900-\U0001FAFF"
            "\U00002600-\U000027BF"
            "]+",
            "",
            cleaned,
        )
        return cleaned.strip()

    def _build_pdf_blocks(self, content: str) -> List[tuple]:
        blocks: List[tuple] = []
        for raw_line in self._normalize_text_for_pdf(content).split("\n"):
            stripped = raw_line.strip()
            if not stripped:
                blocks.append(("spacer", ""))
                continue
            if re.fullmatch(r"-{3,}", stripped):
                blocks.append(("rule", ""))
                continue
            if stripped.startswith("#### "):
                blocks.append(("heading4", self._strip_markdown_for_pdf(stripped[5:])))
                continue
            if stripped.startswith("### "):
                blocks.append(("heading3", self._strip_markdown_for_pdf(stripped[4:])))
                continue
            if stripped.startswith("## "):
                blocks.append(("heading2", self._strip_markdown_for_pdf(stripped[3:])))
                continue
            if stripped.startswith("# "):
                blocks.append(("title", self._strip_markdown_for_pdf(stripped[2:])))
                continue
            if stripped.startswith("> "):
                blocks.append(("quote", self._strip_markdown_for_pdf(stripped[2:])))
                continue
            if re.match(r"^\d+\.\s+", stripped):
                blocks.append(("list", self._strip_markdown_for_pdf(stripped)))
                continue
            if stripped.startswith("- ") or stripped.startswith("* "):
                blocks.append(("bullet", self._strip_markdown_for_pdf(f"• {stripped[2:]}")))
                continue
            blocks.append(("body", self._strip_markdown_for_pdf(stripped)))
        return blocks

    def _build_pdf_attachment(self, content: str) -> Optional[bytes]:
        """Build a readable PDF attachment for email delivery."""
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
            from reportlab.pdfbase.cidfonts import UnicodeCIDFont
            from reportlab.pdfbase import pdfmetrics
            from reportlab.platypus import HRFlowable, Paragraph, SimpleDocTemplate, Spacer
        except ImportError:
            logger.warning("reportlab not installed, PDF attachment disabled")
            return None

        pdf_buffer = BytesIO()
        font_name = "STSong-Light"

        try:
            pdfmetrics.registerFont(UnicodeCIDFont(font_name))
        except Exception:
            pass

        safe_content = self._normalize_text_for_pdf(content)
        blocks = self._build_pdf_blocks(safe_content)
        document = SimpleDocTemplate(
            pdf_buffer,
            pagesize=A4,
            leftMargin=42,
            rightMargin=42,
            topMargin=42,
            bottomMargin=42,
            title="股票智能分析报告",
            author=self._email_config.get('sender_name') or 'daily_stock_analysis',
            subject="daily_stock_analysis 邮件报告附件",
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "PdfTitle",
            parent=styles["Title"],
            fontName=font_name,
            fontSize=18,
            leading=24,
            textColor=colors.HexColor("#111827"),
            spaceAfter=12,
            wordWrap="CJK",
        )
        heading2_style = ParagraphStyle(
            "PdfHeading2",
            parent=styles["Heading2"],
            fontName=font_name,
            fontSize=14,
            leading=20,
            textColor=colors.HexColor("#0f172a"),
            spaceBefore=8,
            spaceAfter=6,
            wordWrap="CJK",
        )
        heading3_style = ParagraphStyle(
            "PdfHeading3",
            parent=styles["Heading3"],
            fontName=font_name,
            fontSize=12.5,
            leading=18,
            textColor=colors.HexColor("#1f2937"),
            spaceBefore=6,
            spaceAfter=4,
            wordWrap="CJK",
        )
        heading4_style = ParagraphStyle(
            "PdfHeading4",
            parent=styles["Heading4"],
            fontName=font_name,
            fontSize=11.5,
            leading=17,
            textColor=colors.HexColor("#374151"),
            spaceBefore=4,
            spaceAfter=3,
            wordWrap="CJK",
        )
        body_style = ParagraphStyle(
            "PdfBody",
            parent=styles["BodyText"],
            fontName=font_name,
            fontSize=10.5,
            leading=17,
            textColor=colors.HexColor("#111827"),
            spaceAfter=3,
            wordWrap="CJK",
        )
        quote_style = ParagraphStyle(
            "PdfQuote",
            parent=body_style,
            leftIndent=12,
            textColor=colors.HexColor("#374151"),
            borderPadding=4,
        )
        list_style = ParagraphStyle(
            "PdfList",
            parent=body_style,
            leftIndent=10,
        )

        style_map = {
            "title": title_style,
            "heading2": heading2_style,
            "heading3": heading3_style,
            "heading4": heading4_style,
            "body": body_style,
            "bullet": list_style,
            "list": list_style,
            "quote": quote_style,
        }

        story = []
        for block_type, block_text in blocks:
            if block_type == "spacer":
                story.append(Spacer(1, 6))
                continue
            if block_type == "rule":
                story.append(Spacer(1, 4))
                story.append(HRFlowable(width="100%", thickness=0.6, color=colors.HexColor("#d1d5db")))
                story.append(Spacer(1, 6))
                continue
            if not block_text:
                continue
            story.append(Paragraph(escape(block_text), style_map[block_type]))

        if not story:
            story.append(Paragraph("暂无可用报告内容。", body_style))

        document.build(story)
        return pdf_buffer.getvalue()

    def _attach_pdf_if_needed(self, msg: MIMEMultipart, markdown_content: Optional[str]) -> None:
        if not self._should_attach_pdf() or not markdown_content:
            return

        pdf_bytes = self._build_pdf_attachment(markdown_content)
        if not pdf_bytes:
            logger.warning("PDF 附件生成失败，回退为普通邮件正文发送")
            return

        pdf_part = MIMEApplication(pdf_bytes, _subtype='pdf')
        pdf_part.add_header('Content-Disposition', 'attachment', filename='stock-analysis-report.pdf')
        msg.attach(pdf_part)

    @staticmethod
    def _close_server(server: Optional[smtplib.SMTP]) -> None:
        """Best-effort SMTP cleanup to avoid leaving sockets open on header/build errors.

        Exceptions from quit()/close() are intentionally silenced — connection may already
        be in a broken state, and there is nothing useful to do at this point.
        """
        if server is None:
            return
        try:
            server.quit()
        except Exception:
            try:
                server.close()
            except Exception:
                pass
    
    def send_to_email(
        self, content: str, subject: Optional[str] = None, receivers: Optional[List[str]] = None
    ) -> bool:
        """
        通过 SMTP 发送邮件（自动识别 SMTP 服务器）
        
        Args:
            content: 邮件内容（支持 Markdown，会转换为 HTML）
            subject: 邮件主题（可选，默认自动生成）
            receivers: 收件人列表（可选，默认使用配置的 receivers）
            
        Returns:
            是否发送成功
        """
        if not self._is_email_configured():
            logger.warning("邮件配置不完整，跳过推送")
            return False
        
        sender = self._email_config['sender']
        password = self._email_config['password']
        receivers = receivers or self._email_config['receivers']
        server: Optional[smtplib.SMTP] = None
        
        try:
            # 生成主题
            if subject is None:
                date_str = datetime.now().strftime('%Y-%m-%d')
                subject = f"📈 股票智能分析报告 - {date_str}"
            
            # 将 Markdown 转换为简单 HTML
            html_content = markdown_to_html_document(content)
            
            # 构建邮件
            msg = MIMEMultipart('alternative')
            msg['Subject'] = Header(subject, 'utf-8')
            msg['From'] = self._format_sender_address(sender)
            msg['To'] = ', '.join(receivers)
            
            # 添加纯文本和 HTML 两个版本
            text_part = MIMEText(content, 'plain', 'utf-8')
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(text_part)
            msg.attach(html_part)
            self._attach_pdf_if_needed(msg, content)
            
            # 自动识别 SMTP 配置
            domain = sender.split('@')[-1].lower()
            smtp_config = SMTP_CONFIGS.get(domain)
            
            if smtp_config:
                smtp_server = smtp_config['server']
                smtp_port = smtp_config['port']
                use_ssl = smtp_config['ssl']
                logger.info(f"自动识别邮箱类型: {domain} -> {smtp_server}:{smtp_port}")
            else:
                # 未知邮箱，尝试通用配置
                smtp_server = f"smtp.{domain}"
                smtp_port = 465
                use_ssl = True
                logger.warning(f"未知邮箱类型 {domain}，尝试通用配置: {smtp_server}:{smtp_port}")
            
            # 根据配置选择连接方式
            if use_ssl:
                # SSL 连接（端口 465）
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)
            else:
                # TLS 连接（端口 587）
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.starttls()
            
            server.login(sender, password)
            server.send_message(msg)
            
            logger.info(f"邮件发送成功，收件人: {receivers}")
            return True
            
        except smtplib.SMTPAuthenticationError:
            logger.error("邮件发送失败：认证错误，请检查邮箱和授权码是否正确")
            return False
        except smtplib.SMTPConnectError as e:
            logger.error(f"邮件发送失败：无法连接 SMTP 服务器 - {e}")
            return False
        except Exception as e:
            logger.error(f"发送邮件失败: {e}")
            return False
        finally:
            self._close_server(server)

    def _send_email_with_inline_image(
        self,
        image_bytes: bytes,
        receivers: Optional[List[str]] = None,
        markdown_content: Optional[str] = None,
    ) -> bool:
        """Send email with inline image attachment (Issue #289)."""
        if not self._is_email_configured():
            return False
        sender = self._email_config['sender']
        password = self._email_config['password']
        receivers = receivers or self._email_config['receivers']
        server: Optional[smtplib.SMTP] = None
        try:
            date_str = datetime.now().strftime('%Y-%m-%d')
            subject = f"📈 股票智能分析报告 - {date_str}"
            msg = MIMEMultipart('related')
            msg['Subject'] = Header(subject, 'utf-8')
            msg['From'] = self._format_sender_address(sender)
            msg['To'] = ', '.join(receivers)

            alt = MIMEMultipart('alternative')
            alt.attach(MIMEText('报告已生成，详见下方图片。', 'plain', 'utf-8'))
            html_body = (
                '<p>报告已生成，详见下方图片（点击可查看大图）：</p>'
                '<p><img src="cid:report-image" alt="股票分析报告" style="max-width:100%%;" /></p>'
            )
            alt.attach(MIMEText(html_body, 'html', 'utf-8'))
            msg.attach(alt)

            img_part = MIMEImage(image_bytes, _subtype='png')
            img_part.add_header('Content-Disposition', 'inline', filename='report.png')
            img_part.add_header('Content-ID', '<report-image>')
            msg.attach(img_part)
            self._attach_pdf_if_needed(msg, markdown_content)

            domain = sender.split('@')[-1].lower()
            smtp_config = SMTP_CONFIGS.get(domain)
            if smtp_config:
                smtp_server, smtp_port = smtp_config['server'], smtp_config['port']
                use_ssl = smtp_config['ssl']
            else:
                smtp_server, smtp_port = f"smtp.{domain}", 465
                use_ssl = True

            if use_ssl:
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)
            else:
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.starttls()
            server.login(sender, password)
            server.send_message(msg)
            logger.info("邮件（内联图片）发送成功，收件人: %s", receivers)
            return True
        except Exception as e:
            logger.error("邮件（内联图片）发送失败: %s", e)
            return False
        finally:
            self._close_server(server)
