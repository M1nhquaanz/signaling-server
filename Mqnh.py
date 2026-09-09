import time
import discord
from discord.ext import commands

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

WELCOME_CHANNEL_ID = 1479386186157133884 

@bot.event
async def on_ready():
    print(f"Bot đã sẵn sàng với tên: {bot.user.name}")

@bot.event
async def on_member_join(member):
    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if not channel:
        return

    embed = discord.Embed(
        title="🎉 CHÀO MỪNG THÀNH VIÊN MỚI! 🎉",
        description=(
            f'Warmest welcome to the **"talents"** who have just joined our **Community crew** – '
            f'the gathering place of souls passionately in love with **Hard Dance and Hardstyle**!\n\n'
            f'👉 Chào mừng {member.mention} đã đến với server!'
        ),
        color=discord.Color.from_rgb(0, 255, 127)
    )

    avatar_url = member.avatar.url if member.avatar else member.default_avatar.url
    embed.set_thumbnail(url=avatar_url)

    guild_icon = member.guild.icon.url if member.guild.icon else None
    embed.set_footer(
        text=f"{member.guild.name} • Thành viên thứ #{len(member.guild.members)}", 
        icon_url=guild_icon
    )

    await channel.send(content=f"Welcome {member.mention}!", embed=embed)

@bot.event
async def on_member_remove(member):
    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if not channel:
        return

    embed = discord.Embed(
        title="👋 TẠM BIỆT",
        description=f"**{member.name}** (`{member.display_name}`) đã rời khỏi server.",
        color=discord.Color.from_rgb(255, 69, 0)
    )

    avatar_url = member.avatar.url if member.avatar else member.default_avatar.url
    embed.set_thumbnail(url=avatar_url)
    
    guild_icon = member.guild.icon.url if member.guild.icon else None
    embed.set_footer(
        text=f"{member.guild.name} • Còn lại {len(member.guild.members)} thành viên", 
        icon_url=guild_icon
    )

    await channel.send(embed=embed)

@bot.command(name="helpme")
async def helpme(ctx):
    embed = discord.Embed(
        title="🎉 BOT CHÀO MỪNG 🎉",
        color=discord.Color.from_rgb(88, 101, 242)
    )
    
    embed.add_field(
        name="📋 Tính năng chính",
        value=(
            "• Bot tự động chào khi có thành viên mới vào server.\n"
            "• Bot tự động báo khi có thành viên rời đi.\n"
            "• Bot chỉ hoạt động trong kênh này."
        ),
        inline=False
    )
    
    embed.add_field(
        name="⚙️ Danh sách lệnh",
        value=(
            "`!ping` - Kiểm tra bot còn hoạt động.\n"
            "`!helpme` - Hiển thị hướng dẫn này."
        ),
        inline=False
    )

    await ctx.send(embed=embed)

@bot.command(name="ping")
async def ping(ctx):
    start_time = time.monotonic()
    message = await ctx.send("Pong! Calculating...")
    end_time = time.monotonic()
    
    ping_ms = round((end_time - start_time) * 1000)
    await message.edit(content=f"Pong! **{ping_ms}ms** (WebSocket: `{round(bot.latency * 1000)}ms`)")

bot.run('MTU0NzI1ODM0NzY1OTU5MTg1MA.GksMrY.V3WsuBWJZ5ruccKIrGQk7YD4x9Gur28Zd4aYxk')
