import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import logging

import ckan.model as model

from ckan.plugins.toolkit import config

import ckan.lib.jobs as jobs
from ckan.lib.dictization import table_dictize
from ckan.model.domain_object import DomainObjectOperation

from ckanext.falkor import tasks2

log = logging.getLogger(__name__)


class FalkorPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic',
            'falkor')


    #IResourceController
    def before_show(self, resource_dict):

        try:
            context = {
                'model': model,
                'session': model.Session,
                'user': tk.c.user,
                'user_obj': tk.c.userobj
            }
            tasks2.documentRead(context,resource_dict)
        except:
            a = 1

        



    #IDomainObjectNotification & #IResourceURLChange
    def notify(self, entity, operation=None):
        context = {'model': model, 'ignore_auth': True, 'defer_commit': True}

        website = "http://ec2-18-134-94-243.eu-west-2.compute.amazonaws.com:5000/"

        if isinstance(entity, model.Resource):
            if not operation:
                #This happens on IResourceURLChange, but I'm not sure whether
                #to make this into a webhook.
                return

            elif operation == DomainObjectOperation.new:
                topic = 'resource/create'
                resource = table_dictize(entity, context)
                tasks2.documentCreate(resource) 

            #resource/document update
            if operation == DomainObjectOperation.changed:
                topic = 'resource/update'
                resource = table_dictize(entity, context)
                tasks2.documentUpdate(resource)
            
            #resource/document delete
            elif operation == DomainObjectOperation.deleted:
                topic = 'resource/delete'
                resource = table_dictize(entity, context)
                tasks2.documentDelete(resource) 
            else:
                return

        if isinstance(entity, model.Package):
            #Dataset create
            if operation == DomainObjectOperation.new:
                topic = 'dataset/create'
                resource = table_dictize(entity, context)
                tasks2.datasetCreate(resource) 

            #Dataset update
            #Most likely not required as falkor doesnt allow updating datasets
            elif operation == DomainObjectOperation.changed:
                topic = 'dataset/update'

            #Dataset delete
            #Most likely not required as falkor doesnt allow deleting datasets
            elif operation == DomainObjectOperation.deleted:
                topic = 'dataset/delete'

            else:
                return
