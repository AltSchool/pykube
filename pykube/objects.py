import copy
import json
import time

import jsonpatch

from six.moves.urllib.parse import urlencode
from .exceptions import ObjectDoesNotExist
from .query import ObjectManager


DEFAULT_NAMESPACE = "default"


class APIObject(object):

    objects = ObjectManager()
    base = None
    namespace = None

    def __init__(self, api, obj):
        self.api = api
        self.set_obj(obj)

    def set_obj(self, obj):
        self.obj = obj
        self._original_obj = copy.deepcopy(obj)

    @property
    def name(self):
        return self.obj["metadata"]["name"]

    @property
    def annotations(self):
        return self.obj["metadata"].get("annotations", {})

    def api_kwargs(self, **kwargs):
        kw = {}
        collection = kwargs.pop("collection", False)
        if collection:
            kw["url"] = self.endpoint
        else:
            kw["url"] = "{}/{}".format(self.endpoint, self._original_obj["metadata"]["name"])
        if self.base:
            kw["base"] = self.base
        kw["version"] = self.version
        if self.namespace is not None:
            kw["namespace"] = self.namespace

        subcommand = kwargs.pop('subcommand', None)
        if subcommand:
            kw['url'] = "{}/{}".format(kw['url'], subcommand)

        query_params = kwargs.pop('query_params', None)
        if query_params:
            query_params = urlencode(query_params)
            kw['url'] = "{}?{}".format(kw['url'], query_params)
        kw.update(kwargs)
        return kw

    def exists(self, ensure=False):
        r = self.api.get(**self.api_kwargs())
        if r.status_code not in {200, 404}:
            self.api.raise_for_status(r)
        if not r.ok:
            if ensure:
                raise ObjectDoesNotExist("{} does not exist.".format(self.name))
            else:
                return False
        return True

    def create(self):
        r = self.api.post(**self.api_kwargs(data=json.dumps(self.obj), collection=True))
        self.api.raise_for_status(r)
        self.set_obj(r.json())

    def reload(self):
        r = self.api.get(**self.api_kwargs())
        self.api.raise_for_status(r)
        self.set_obj(r.json())

    def update(self):
        patch = jsonpatch.make_patch(self._original_obj, self.obj)
        r = self.api.patch(**self.api_kwargs(
            headers={"Content-Type": "application/json-patch+json"},
            data=str(patch),
        ))
        self.api.raise_for_status(r)
        self.set_obj(r.json())

    def delete(self):
        r = self.api.delete(**self.api_kwargs())
        if r.status_code != 404:
            self.api.raise_for_status(r)


class NamespacedAPIObject(APIObject):

    objects = ObjectManager(namespace=DEFAULT_NAMESPACE)

    @property
    def namespace(self):
        if self.obj["metadata"].get("namespace"):
            return self.obj["metadata"]["namespace"]
        else:
            return DEFAULT_NAMESPACE


class ReplicatedAPIObject(object):

    @property
    def replicas(self):
        return self.obj["spec"]["replicas"]

    @replicas.setter
    def replicas(self, value):
        self.obj["spec"]["replicas"] = value


class ConfigMap(NamespacedAPIObject):

    version = "v1"
    endpoint = "configmaps"
    kind = "ConfigMap"


class DaemonSet(NamespacedAPIObject):

    version = "extensions/v1beta1"
    endpoint = "daemonsets"
    kind = "DaemonSet"


class Deployment(NamespacedAPIObject, ReplicatedAPIObject):

    version = "extensions/v1beta1"
    endpoint = "deployments"
    kind = "Deployment"


class Endpoint(NamespacedAPIObject):

    version = "v1"
    endpoint = "endpoints"
    kind = "Endpoint"


class Ingress(NamespacedAPIObject):

    version = "extensions/v1beta1"
    endpoint = "ingresses"
    kind = "Ingress"


class Job(NamespacedAPIObject):

    version = "batch/v1"
    endpoint = "jobs"
    kind = "Job"

    def scale(self, replicas=None):
        """
        Scales a job as it would be done by kubectl scale --replicas=num Jobs/myjob.
        Replicas can start from zero.
        """
        parallelism = replicas
        # we use parallelism from now on because this is what it is altered at
        # the API level by the kubectl call
        if parallelism is None:
            parallelism = self.obj["spec"]["parallelism"]
        self.exists(ensure=True)
        self.obj["spec"]["parallelism"] = parallelism
        self.update()
        while True:
            self.reload()
            if self.self.obj["spec"]["parallelism"] == parallelism:
                break
            time.sleep(1)


class Namespace(APIObject):

    version = "v1"
    endpoint = "namespaces"
    kind = "Namespace"


class Node(APIObject):

    version = "v1"
    endpoint = "nodes"
    kind = "Node"


class Pod(NamespacedAPIObject):

    version = "v1"
    endpoint = "pods"
    kind = "Pod"

    @property
    def ready(self):
        cs = self.obj["status"]["conditions"]
        condition = next((c for c in cs if c["type"] == "Ready"), None)
        return condition is not None and condition["status"] == "True"

    def execute(
        self,
        command,
        stdin=None,
        stdout=None,
        stderr=None,
        tty=None,
        container=None,
    ):
        params = {}
        params['command'] = str(command)
        if stdin is not None:
            params['stdin'] = str(stdin).lower()
        if stdout is not None:
            params['stdout'] = str(stdout).lower()
        if stderr is not None:
            params['stderr'] = str(stderr).lower()
        if tty is not None:
            params['tty'] = tty
        if container is not None:
            params['container'] = container

        response = self.api.get(
            **self.api_kwargs(
                subcommand='exec',
                query_params=params
            )
        )

        response.raise_for_status()
        return response.text


class ReplicationController(NamespacedAPIObject, ReplicatedAPIObject):

    version = "v1"
    endpoint = "replicationcontrollers"
    kind = "ReplicationController"

    def scale(self, replicas=None):
        if replicas is None:
            replicas = self.replicas
        self.exists(ensure=True)
        self.replicas = replicas
        self.update()
        while True:
            self.reload()
            if self.replicas == replicas:
                break
            time.sleep(1)


class ReplicaSet(NamespacedAPIObject, ReplicatedAPIObject):

    version = "extensions/v1beta1"
    endpoint = "replicasets"
    kind = "ReplicaSet"


class Secret(NamespacedAPIObject):

    version = "v1"
    endpoint = "secrets"
    kind = "Secret"


class Service(NamespacedAPIObject):

    version = "v1"
    endpoint = "services"
    kind = "Service"


class PersistentVolume(APIObject):

    version = "v1"
    endpoint = "persistentvolumes"
    kind = "PersistentVolume"


class PersistentVolumeClaim(NamespacedAPIObject):

    version = "v1"
    endpoint = "persistentvolumeclaims"
    kind = "PersistentVolumeClaim"
