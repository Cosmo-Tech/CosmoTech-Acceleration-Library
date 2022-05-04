import os


class MultiEnvironment:

    def __init__(self):
        self.api_host = None
        self.api_scope = None

        for host_var in ['COSMOTECH_API_SCOPE', 'CSM_API_SCOPE']:
            if host_var in os.environ:
                self.api_scope = os.environ.get(host_var)
                break

        for host_var in ['COSMOTECH_API_HOST', 'CSM_API_HOST']:
            if host_var in os.environ:
                self.api_host = os.environ.get(host_var)
                break
