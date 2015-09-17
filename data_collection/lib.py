from datetime import datetime, timedelta
import json
import urllib2
import ldap


def call_rm_restful_api(active_rm_ip, active_rm_port, url):
    """
        Call YARN resource manager restful api.
        api refs : https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
        @param active_rm_ip: active IP of resource manager. ex:10.10.10.10
        @param active_rm_port: the port of resource manager. resource manager default port is 8088
        @param url: restful api path. ex: /ws/v1/cluster/info
        @return: dictionary format data
                     { 'success':False, 'message':'error message' }
                     { 'success':True, 'message':{...} }
        """
    result = {'success':False, 'message':''}
    url = "http://{0}:{1}{2}".format(active_rm_ip, str(active_rm_port), url)
    #print( "[DEBUG] RM request = {0}".format(url) )
    req = urllib2.Request(url)
    try:
        response = urllib2.urlopen(req, timeout=3)
        data = response.read()
        if response.getcode() == 200:
            #print( "[DEBUG] RM response = {0}".format(data) )
            result['success'] = True
            result['message'] = json.loads(data)
        else:
            message = "RM request error : {0}".format(url)
            print( "[ERROR] {0}. {1}".format(message, str(data)) )
            result['success'] = False
            result['message'] = message
    except Exception, e:
        message = "RM connection error : {0}".format(url)
        print( "[ERROR] {0}. {1}".format(message, str(e)) )
        result['success'] = False
        result['message'] = message
    return result


def datetime_to_epoch_ms(date_time):
    """
        Change datetime to epoch ms
        @param date_time: the specific datetime. ex: datetime(2015,9,17)
        @return: epoch ms of specific datetime
        """
    base = datetime(1970,1,1,00,00)
    td = date_time - base
    return ( (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6) * 1000


def epoch_ms_to_datetime(epoch_ms):
    """
        Change epoch ms to datetime
        @param epoch_ms: the specific epoch ms. ex: 1442296120406
        @return: datetime of specific epoch ms
        """
    return datetime.fromtimestamp(epoch_ms / 1000)


def applications_statistics(active_rm_ip, active_rm_port, from_time, to_time, filter_dict=None):
    """
        Call resource manager restful api to get completed application information on specific conditions and accumulate it..
        @param active_rm_ip: the IP of active resource manager
        @param active_rm_port: port of of active resource manager. default port is 8088
        @param from_time: Application finish time after this time. type:datetime. time zone:UTC
        @param to_time: Application finish time before this time. type:datetime. time zone:UTC
        @param filter_dict: filter information. type:dictionary
        @return: dictionary format data
                    { 'success':Fasle, 'message':'error message' }
                    { 'success':True, 'message':{....} }
        """
    #
    # init
    result = {'success':False, 'message':''}
    statistics = {}
    #
    # change datetime(UTC) to epoch ms
    from_time_epoch_ms = datetime_to_epoch_ms(from_time)
    to_time_epoch_ms = datetime_to_epoch_ms(to_time)
    #
    # according to filter conditions, generate RM restful api url
    url = "/ws/v1/cluster/apps?finishedTimeBegin={0}&finishedTimeEnd={1}".format( str(from_time_epoch_ms), str(to_time_epoch_ms) )
    if filter_dict:
        if filter_dict.has_key('app_type'):
            url += "&applicationTypes={0}".format( filter_dict['app_type'] )
        if filter_dict.has_key('app_state'):
            url += "&states={0}".format( ",".join(filter_dict['app_state']) )
    #
    # call RM restful api and get response to accumulate
    response = call_rm_restful_api(active_rm_ip, active_rm_port, url)
    if response['success']:
        result['success'] = True
        if response['message'].has_key('apps') and response['message']['apps']:
            for app in response['message']['apps']['app']:
                queue = app['queue']
                start_time_epoch_ms = app['startedTime']
                finish_time_epoch_ms = app['finishedTime']
                duration = epoch_ms_to_datetime(finish_time_epoch_ms) - epoch_ms_to_datetime(start_time_epoch_ms)
                finalStatus = app['finalStatus']
                app_type = app['applicationType']
                memorySeconds = app['memorySeconds']
                vcoreSeconds = app['vcoreSeconds']
                #
                if (not filter_dict) or (filter_dict and not filter_dict.has_key('queue_name')) or \
                   (filter_dict and filter_dict.has_key('queue_name') and queue == filter_dict['queue_name']):
                    if not statistics.has_key(queue):
                        statistics[queue] = { 'apps':{} }
                    if not statistics[queue]['apps'].has_key(app_type):
                        statistics[queue]['apps'][app_type] = { 'final_status':{} }
                    if not statistics[queue]['apps'][app_type]['final_status'].has_key(finalStatus):
                        statistics[queue]['apps'][app_type]['final_status'][finalStatus] = { 'applications':1, 'duration':duration, 'memorySeconds':memorySeconds, 'vcoreSeconds':vcoreSeconds }
                    else:
                        statistics[queue]['apps'][app_type]['final_status'][finalStatus]['applications'] += 1
                        statistics[queue]['apps'][app_type]['final_status'][finalStatus]['duration'] += duration
                        statistics[queue]['apps'][app_type]['final_status'][finalStatus]['memorySeconds'] += memorySeconds
                        statistics[queue]['apps'][app_type]['final_status'][finalStatus]['vcoreSeconds'] += vcoreSeconds
        result['message'] = statistics
    else:
        result['success'] = False
        result['message'] = response['message']
    return result


# refs : https://www.packtpub.com/books/content/python-ldap-applications-part-1-installing-and-configuring-python-ldap-library-and-bin
# refs : http://www.linuxjournal.com/article/6988
# refs : http://qiita.com/mykysyk@github/items/38cfc239e6043d5b7346
def create_ldap_connection(host, username, password):
    """
        create LDAP connection
        @param host: ldap host ip. ex:127.0.0.1
        @param username: username on ldap
        @param password: the password of username
        @return: ldap connection or False
        """
    ldap_connection = ldap.initialize( "ldap://{0}".format(host) )
    try:
        ldap_connection.bind_s( "uid={0},ou=People,dc=cht,dc=local".format(username), password )
    except ldap.LDAPError, e:
        message = "LDAP connection error"
        if type(e.message) == dict and e.message.has_key('desc'):
            print( "[ERROR] {0}. {1}".format(message, e.message['desc']) )
        else:
            print( "[ERROR] {0}. {1}".format(message, str(e)) )
        return False
    return ldap_connection


def query_group_of_user(ldap_connection, user_account):
    """
        get the group of specific user account
        @param ldap_connection: ldap connection
        @param user_account: user account
        @return: group name. if can't get the group of specific user account, return empty string
        """
    user_base_dn = "ou=People,dc=cht,dc=local"
    group_base_dn = "ou=Group,dc=cht,dc=local"
    group_name = ""
    user_search_result = ldap_connection.search_s(user_base_dn, ldap.SCOPE_SUBTREE, "uid={0}".format(user_account), ['sn','gidNumber'])
    if len(user_search_result) > 0:
        (schema, info) = user_search_result[0]
        if info.has_key('gidNumber') and len( info['gidNumber'] ) > 0:
            gid = info['gidNumber'][0]
            group_search_result = ldap_connection.search_s(group_base_dn, ldap.SCOPE_SUBTREE, "gidNumber={0}".format(str(gid)), ['cn'])
            if len(group_search_result) > 0:
                (schema, info) = group_search_result[0]
                if info.has_key('cn') and len( info['cn'] ) > 0:
                    group_name = info['cn'][0]
    return group_name


def transform_queue_view_response(response):
    """
        transform duration type from datetime.timedelta to string
        @param response: queue view result. type:dictionary
        @return: transformed queue view result. type:dictionary
        """
    for queue, queue_info in response.items():
        for app_type, app_info in queue_info['apps'].items():
            for app_state, data in app_info['final_status'].items():
                for key in data:
                    if key == 'duration':
                        data[key] = str(data[key])
    return response


def transform_project_view_response(response):
    """
        transform duration type from datetime.timedelta to string
        @param response: project view result. type:dictionary
        @return: transformed project view result. type:dictionary
        """
    for project, project_info in response.items():
        for app_state, app_info in project_info['apps'].items():
            for key in app_info:
                if key == 'duration':
                    app_info[key] = str(app_info[key])
    return response
