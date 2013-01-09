import time
import json

from bitdeli import profile_events
from bitdeli.protocol import params
from bitdeli.chunkedlist import ChunkedList

PROFILE_RETENTION = params()['plan']['retention-days']

now = time.time() * 1000

for profile, sessions in profile_events():
    psessions = profile.get('sessions')
    if psessions == None:
        psessions = profile['sessions'] = ChunkedList()
    psessions.drop_chunks(lambda x:\
                          now - json.loads(x.data)['t'] <= PROFILE_RETENTION)
    psessions.push(s.object for s in sessions)
    profile.set_expire(PROFILE_RETENTION)



