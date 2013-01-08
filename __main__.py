import time

from bitdeli import profile_events
from bitdeli.protocol import params
from bitdeli.chunkedlist import ChunkedList

PROFILE_RETENTION = params()['plan']['retention-days']

now = time.time() * 1000

for profile, sessions in profile_events():
    psessions = profile.get('sessions')
    if psessions == None:
        psessions = profile['sessions'] = ChunkedList()
    psessions.push(sessions)
    psessions.drop_chunks(lambda x: now - x['t'] <= PROFILE_RETENTION)
    profile.set_expire(PROFILE_RETENTION)



