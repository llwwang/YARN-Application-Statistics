from .lib import create_ldap_connection, applications_statistics, query_group_of_user, transform_queue_view_response, transform_project_view_response
from django.shortcuts import render
from django.http import HttpResponse
from django.conf import settings
from datetime import datetime, timedelta
from cm_api.api_client import ApiResource
import json
import ConfigParser
import os


def index(request):
    """
        System index web page
        """
    return render(request, 'index.html')


def api_data_collection(request):
    """
        Application information collection restful api. Query completed application information on specific conditions and accumulate it.
        @method: GET
        @param from_time: Application finish time after this time. format : "%d/%m/%Y %H:%M". time zone=UTC+8
        @param end_time: Application finish time before this time. format : "%d/%m/%Y %H:%M". time zone=UTC+8
        @param queue_name: Query completed application information on specific queue name.
        @param app_type: Query completed application information on specific application type.
        @param app_state: Query completed application information on specific application states. specified as a comma-separated list. ex: FINISHED,FAILED,KILLED
        @return: json data
                    { "success":False, "message":"error message" }
                    { "success":True, "message": { "queue_view":{...}, "group_view":{...} } }
        """
    if request.method == "GET":
        response = {'success':False, 'message':''}
        filter_dict = {}
        if "queue_name" in request.GET:
            filter_dict['queue_name'] = request.GET.get('queue_name')
        if "app_type" in request.GET:
            filter_dict['app_type'] = request.GET.get('app_type')
        if "app_state" in request.GET:
            filter_dict['app_state'] = request.GET.get('app_state').split(',')
        #
        # time zone = Asia/Taipei = UTC+8
        from_time = datetime.strptime(request.GET.get('from_time'), "%d/%m/%Y %H:%M") - timedelta(hours=8)
        to_time = datetime.strptime(request.GET.get('end_time'), "%d/%m/%Y %H:%M") - timedelta(hours=8)
        #
        # get config
        config = ConfigParser.ConfigParser()
        config.read( os.path.join(settings.BASE_DIR, "cluster.ini") )
        cm_host = config.get("CM", "cm.host")
        cm_port = config.get("CM", "cm.port")
        cm_version = config.get("CM", "cm.version")
        cm_username = config.get("CM", "cm.username")
        cm_password = config.get("CM", "cm.password")
        #
        cluster_name = config.get("Cluster", "cluster.name")
        yarn_name = config.get("Cluster", "cluster.yarn.name")
        #
        ldap_host = config.get("Ldap", "ldap.host")
        ldap_username = config.get("Ldap", "ldap.username")
        ldap_password = config.get("Ldap", "ldap.password")
        #
        # get active resource manager info
        try:
            cm_api = ApiResource( cm_host, int(cm_port), username=cm_username, password=cm_password, version=int(cm_version) )
            cm_cluster_obj = cm_api.get_cluster(name=cluster_name)
            cm_yarn_obj = cm_cluster_obj.get_service(name=yarn_name)
            #
            find_active_rm = False
            for rm in cm_yarn_obj.get_roles_by_type(role_type="RESOURCEMANAGER"):
                if rm.haStatus == "ACTIVE":
                    host = cm_api.get_host(rm.hostRef.hostId)
                    active_rm_ip = host.ipAddress
                    active_rm_port = 8088
                    find_active_rm = True
            #
            if not find_active_rm:
                message = "can not find active rm"
                print( "[ERROR] " + message )
                response['success'] = False
                response['message'] = message
                return HttpResponse( json.dumps(response) )
        except Exception, e:
            message = "can not get cm yarn object"
            print( "[ERROR] " + message + str(e) )
            response['success'] = False
            response['message'] = message
            return HttpResponse( json.dumps(response) )
        #
        # all application statistics
        statistics_response = applications_statistics(active_rm_ip, active_rm_port, from_time, to_time, filter_dict)
        if statistics_response['success']:
            #
            # create ldap connection. access ldap to get group of account
            if create_ldap_connection(ldap_host, ldap_username, ldap_password):
                ldap_connection = create_ldap_connection(ldap_host, ldap_username, ldap_password)
            else:
                message = "can not connect to ldap://" + ldap_host
                response['success'] = False
                response['message'] = message
                return HttpResponse( json.dumps(response) )
            #
            # init queue view result & group view result
            queue_view_final_result = statistics_response['message']
            group_view_final_result = {}
            #
            # load project define json file. get the project name of group
            with open(os.path.join(settings.BASE_DIR, "project_define.json"), 'r') as f:
                project_define = json.load(f)
            #
            # add group information to queue view result and accumulate the result by group
            for queue, queue_info in queue_view_final_result.items():
                #
                # queue naming : root.SYSTEM.<account> , root.PERSONAL.<account>
                queue_view_final_result[queue]['group'] = ''
                if len( queue.split('.') ) == 3 and queue.split('.')[0] == "root" and ( queue.split('.')[1] == "SYSTEM" or queue.split('.')[1] == "PERSONAL" ):
                    queue_view_final_result[queue]['account'] = queue.split('.')[-1]
                    group = query_group_of_user(ldap_connection, queue_view_final_result[queue]['account'])
                    queue_view_final_result[queue]['group'] = group
                    if not group_view_final_result.has_key(group):
                        group_view_final_result[group] = { 'apps':{}, 'queues':[], 'name':'' }
                    group_view_final_result[group]['queues'].append(queue)
                    if project_define.has_key(group):
                        group_view_final_result[group]['name'] = project_define[group]
                    #
                    for app_type, app_info in queue_info['apps'].items():
                        for app_state, data in app_info['final_status'].items():
                            if not group_view_final_result[group]['apps'].has_key(app_state):
                                group_view_final_result[group]['apps'][app_state] = {}
                            for key in data:
                                if not group_view_final_result[group]['apps'][app_state].has_key(key):
                                    group_view_final_result[group]['apps'][app_state][key] = data[key]
                                else:
                                    group_view_final_result[group]['apps'][app_state][key] += data[key]
            #
            # after finish to accumulate all result, unbind ldap connection
            ldap_connection.unbind()
        else:
            response['success'] = False
            response['message'] = statistics_response['message']
            return HttpResponse( json.dumps(response) )
        #
        # transform duration type from datetime.timedelta to string
        queue_view_final_result = transform_queue_view_response(queue_view_final_result)
        group_view_final_result = transform_project_view_response(group_view_final_result)
        #
        response['success'] = True
        response['message'] = {}
        response['message']['queue_view'] = queue_view_final_result
        response['message']['group_view'] = group_view_final_result
        print json.dumps("[DEBUG] response = " + json.dumps(response))
        return HttpResponse( json.dumps(response) )
