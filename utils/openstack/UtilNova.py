from ConnectionGetter import *
import swiftclient
import subprocess
import os
import time

DEF_WAITING_TIME = 2

class UtilNova():

    def __init__(self, connection):
        self.connection = connection

    def attach_volume(self, server_id, volume_id):

        print 'Attaching volume of id %s, to server of id %s' % (volume_id, server_id)
        self.connection.volumes.create_server_volume(server_id, volume_id, None)
        time.sleep(DEF_WAITING_TIME)
        print 'Success!'

    def detache_volume(self, server_id, volume_id):

        print 'Detaching volume of id %s, to server of id %s' % (volume_id, server_id)
        self.connection.volumes.delete_server_volume(server_id, volume_id)
        time.sleep(DEF_WAITING_TIME)
        print 'Success!'


