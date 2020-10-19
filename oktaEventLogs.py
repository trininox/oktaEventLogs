#!/usr/bin/env python3

import sys,os,getopt
import traceback
import os
from datetime import datetime, timedelta
import requests
import json

sys.path.insert(0, './ds-integration')
from DefenseStorm import DefenseStorm

class integration(object):

    JSON_field_mappings = {
        'published' : 'timestamp',
        'displayMessage' : 'message'
    }

    def getEvents(self):
        events_url = 'https://' + self.api_uri + '/api/v1/events'
        logs_url = 'https://' + self.api_uri + '/api/v1/logs'
        headers = { 'Authorization': 'SSWS ' + self.api_token }
        params = {'startDate': self.mystate.isoformat()[:-3] + 'Z' }

        try:
            self.ds.log('INFO','Sending requests {0}'.format(events_url))
            events = requests.get(events_url, headers=headers, params=params)
        except Exception as e:
            self.ds.log('ERROR', "Exception {0}".format(str(e)))
            return []

        if not events or events.status_code != 200:
            self.ds.log('WARNING',
                "Received unexpected " + str(events) + " response from Okta Server {0}.".format(
                events_url))
            return []
        ret_list = []

        for e in events.json():
            if e == 'errorCode':
                break
            data = json.loads(json.dumps(e))
            data['category'] = 'events'
            e = json.dumps(data)
            ret_list.append(e)

        while 'next' in events.links:
            try:
                self.ds.log('INFO','Sending requests {0}'.format(events.links['next']['url']))
                events = requests.get(events.links['next']['url'], headers=headers)
            except Exception as e:
                self.ds.log('ERROR', "Exception {0}".format(str(e)))
                return []
            if not events or events.status_code != 200:
                self.ds.log('WARNING',
                    "Received unexpected " + str(events) + " response from Okta Server {0}.".format(
                    alerts_url))
                return []
            for e in events.json():
                if e == 'errorCode':
                    break
                e['category'] = 'events'
                ret_list.append(e)
        return ret_list

    def getLogs(self):
        logs_url = 'https://' + self.api_uri + '/api/v1/logs'
        headers = { 'Authorization': 'SSWS ' + self.api_token }
        params = {'startDate': self.mystate.isoformat()[:-3] + 'Z' }

        try:
            self.ds.log('INFO','Sending requests {0}'.format(logs_url))
            events = requests.get(logs_url, headers=headers, params=params)
        except Exception as e:
            self.ds.log('ERROR', "Exception {0}".format(str(e)))
            return []
        if not events or events.status_code != 200:
            self.ds.log('WARNING',
                "Received unexpected " + str(events) + " response from Okta Server {0}.".format(
                logs_url))
            return []

        ret_list = []

        for e in events.json():
            if e == 'errorCode':
                break
            e['category'] = 'events'
            ret_list.append(e)

        while 'next' in events.links:
            #print(events.links)
            try:
                self.ds.log('INFO','Sending requests {0}'.format(events.links['next']['url']))
                events = requests.get(events.links['next']['url'], headers=headers)
            except Exception as e:
                self.ds.log('ERROR', "Exception {0}".format(str(e)))
                return []
            if not events or events.status_code != 200:
                self.ds.log('WARNING',
                    "Received unexpected " + str(events) + " response from Okta Server {0}.".format(
                    events.status_code))
                return []
            for e in events.json():
                if e == 'errorCode':
                    break
                e['category'] = 'logs'
                ret_list.append(e)
        return ret_list


    def run(self):
        self.ds.log('INFO', 'Getting Okta Logs')
        log_list = self.getLogs()
        self.ds.log('INFO', 'Getting Okta Events')
        event_list = self.getEvents()
        #self.ds.writeCEFEvent()
        for event in event_list:
            self.ds.writeJSONEvent(event, JSON_field_mappings = self.JSON_field_mappings)
        for event in log_list:
            self.ds.writeJSONEvent(event, JSON_field_mappings = self.JSON_field_mappings)
        self.ds.set_state(self.state_dir, self.newstate)
    
    def usage(self):
        print
        print(os.path.basename(__file__))
        print
        print('  No Options: Run a normal cycle')
        print
        print('  -t    Testing mode.  Do all the work but do not send events to GRID via ')
        print('        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\'')
        print('        in the current directory')
        print
        print('  -l    Log to stdout instead of syslog Local6')
        print
    
    def __init__(self, argv):

        self.testing = False
        self.send_syslog = True
        self.ds = None
    
        try:
            opts, args = getopt.getopt(argv,"htnld:",["datedir="])
        except getopt.GetoptError:
            self.usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                self.usage()
                sys.exit()
            elif opt in ("-t"):
                self.testing = True
            elif opt in ("-l"):
                self.send_syslog = False
    
        try:
            self.ds = DefenseStorm('oktaEventLogs', testing=self.testing, send_syslog = self.send_syslog)
        except Exception as e:
            traceback.print_exc()
            try:
                self.ds.log('ERROR', 'ERROR: ' + str(e))
            except:
                pass

        self.api_token = self.ds.config_get('okta', 'api_token')
        self.api_uri = self.ds.config_get('okta', 'api_uri')
        self.state_dir = os.path.join(self.ds.config_get('okta', 'APP_PATH'), 'state')
        self.mystate = self.ds.get_state(self.state_dir)
        self.newstate = datetime.now()

        if self.mystate == None:
            self.mystate = self.newstate - timedelta(0,600)


if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
