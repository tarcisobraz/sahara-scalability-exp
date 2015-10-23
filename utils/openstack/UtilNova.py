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
        attached = False
        while (not attached):
            time.sleep(0.5)
            server_volumes_ids = [v.id for v in self.connection.volumes.get_server_volumes(server_id)]
            print "Server Volumes: ", server_volumes_ids
            if (volume_id in server_volumes_ids):
                attached = True
			
        print 'Success!'

    def detache_volume(self, server_id, volume_id):

        print 'Detaching volume of id %s, to server of id %s' % (volume_id, server_id)
        self.connection.volumes.delete_server_volume(server_id, volume_id)
        time.sleep(DEF_WAITING_TIME)
        print 'Success!'


