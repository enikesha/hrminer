import aiohttp
import argparse
import asyncio
import os
import re
from collections import defaultdict
from telethon import TelegramClient, utils
from telethon.tl.types import Chat, Channel
from telethon import functions, types

api_id = os.environ.get('API_ID')
api_hash = os.environ.get('API_HASH')

async def print_chats(args):
    async with TelegramClient('hrminer', api_id, api_hash) as client:
        async for dlg in  client.iter_dialogs(limit=50):
            if isinstance(dlg.entity, Chat) or (isinstance(dlg.entity, Channel) and dlg.entity.megagroup):
                print(f"{dlg.id:<16} {dlg.name}")

async def print_top(args):
    chats = args.chat_id
    async with TelegramClient('hrminer', api_id, api_hash) as client:
        for chat_id in chats:
            chat = await client.get_entity(chat_id)
            print(f"{chat_id:<20} {utils.get_display_name(chat)}")
            print()

            messages = defaultdict(int)
            chars_true = defaultdict(int)
            words_true = defaultdict(int)
            skipped = defaultdict(int)
            last_date = None
            idx = 0
            async for msg in client.iter_messages(chat_id, limit=args.messages):
                uid = msg.from_id
                messages[uid] += 1
                text = msg.message or ''
                words_cnt = len(text.split())
                if msg.fwd_from is None and msg.via_bot_id is None and words_cnt < 50:
                    chars_true[uid] += len(text)
                    words_true[uid] += words_cnt
                else:
                    skipped[uid] += 1
                last_date = msg.date
                idx += 1
                if (idx % 1000) == 0:
                    print(f"Got {idx} messages")

            top_users = sorted(chars_true.items(), key=lambda p: p[1], reverse=True)[:args.top]
            user_infos = await asyncio.gather(*map(lambda p: client.get_entity(p[0]), top_users))
            users_filename = f"top_{args.top}_users_of_{chat_id}_{args.messages}"

            print(f"Top contributors for last {idx} messages starting from {last_date}")
            print("{:>10} {:<20} {:<20} {:>8} {:>6} {:>6} {:>6}".format('user_id', 'username', 'display_name', 'chars', 'words', 'msgs', 'skip'))
            print('-' * 82)
            with open(users_filename, 'wt') as f:
                for user in user_infos:
                    username = user.username or ''
                    print(f"{user.id:10} {username:<20} {utils.get_display_name(user):<20} {chars_true[user.id]:8} {words_true[user.id]:6} {messages[user.id]:6} {skipped[user.id]:6}")
                    f.write(f"{user.id}\t{username}\t{utils.get_display_name(user)}\n")
            print("\n")

URL_RE = re.compile(r"(?:http(?:s)?:\/\/.)?(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b(?:[-a-zA-Z0-9@:%_\+.~#?&//=]*)")

async def try_head(session, key, url):
    async with session.get(url) as response:
        code = response.status
        if code == 404:
            url = None
        return (key, url)

async def enrich(args):
    user_ids = []
    with open(args.users_file, 'rt') as f:
        for line in f:
            user_ids.append(int(line.split()[0]))

    rich = {}
    async with TelegramClient('hrminer', api_id, api_hash) as client:
        user_full_infos = await asyncio.gather(*map(lambda uid: client(functions.users.GetFullUserRequest(uid)), user_ids))

        async with aiohttp.ClientSession() as session:
            requests = []
            for info in user_full_infos:
                uid = info.user.id
                username = info.user.username or ''
                rich[uid] = {'username': username,
                             'display_name': utils.get_display_name(info.user),
                             'phone': info.user.phone or '',
                             'tg': f"http://t.me/{username}" if username else '',
                             'links': URL_RE.findall(info.about or '') or None}
                if username:
                    requests.extend([
                        try_head(session, (uid, 'vk'), f"https://vk.com/{username}"),
                        try_head(session, (uid, 'twitter'), f"https://twitter.com/{username}"),
                        try_head(session, (uid, 'github'), f"https://github.com/{username}"),
                    ])

            for (uid, type), url in await asyncio.gather(*requests):
                if url is not None:
                    rich[uid][type] = url

    for uid in user_ids:
        r = rich[uid]
        print(f"{r['display_name']}")
        for key in ('username', 'phone', 'links', 'tg', 'vk', 'twitter', 'github'):
            if key in r and r[key]:
                print(f"{key:>8}: {r[key]}")
        print()

async def main():
    parser = argparse.ArgumentParser(description='Calculate telegram chats users stat and gather additional info.')
    subparsers = parser.add_subparsers(title='main mode')

    parser_chats = subparsers.add_parser('chats', help='Show list of your chats', description='Show chats list')
    parser_chats.set_defaults(func=print_chats)

    parser_top = subparsers.add_parser('top', help='Calculate top chats users', description='Calculate top chats users')
    parser_top.set_defaults(func=print_top)
    parser_top.add_argument('-m', '--messages', type=int,
                            help='Number of messages to retrieve. Beware, 1k msgs take 10 sec to load. Default: 20000',
                            default=20000)
    parser_top.add_argument('-t', '--top', type=int, help='Number of top users to show. Default: 20', default=20)
    parser_top.add_argument('chat_id', type=int, help="chat_id from 'chats' output", nargs='+')

    parser_enrich = subparsers.add_parser('enrich',
                                          help='Enrich users with links to other sources (vk, twitter, github)',
                                          description='Enrich users with links to other sources (vk, twitter, github)')
    parser_enrich.set_defaults(func=enrich)
    parser_enrich.add_argument('users_file', help="Results filename of 'top' command")

    args = parser.parse_args()
    await args.func(args)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
