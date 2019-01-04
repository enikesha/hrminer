import aiohttp
import asyncio
import base64
import os

KEY = os.environ.get('TWITTER_KEY')
SECRET = os.environ.get('TWITTER_SECRET')

async def main():
    async with aiohttp.ClientSession() as session:
        auth = base64.b64encode(f"{KEY}:{SECRET}".encode('utf-8'))
        api_url = 'https://api.twitter.com/oauth2/token'
        headers = {'authorization': f"Basic {auth.decode('utf-8')}"}
        data = {'grant_type': 'client_credentials'}
        async with session.post(api_url, headers=headers, data=data) as response:
            res = await response.json()
            if 'access_token' in res:
                print(res['access_token'])
            else:
                print(res)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
