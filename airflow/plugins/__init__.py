from airflow.plugins_manager import AirflowPlugin

import operators

#from redshift_plugin.operators.s3_to_redshift_operator import S3ToRedshiftOperator
#from redshift_plugin.macros.redshift_auth import redshift_auth

class CapstoneProjectPlugin(AirflowPlugin):
    name = "capstone_project_plugin"
    operators = [operators.S3ToRedshiftOperator,
                operators.DataQualityOperator,
                operators.LoadDimensionOperator,
                operators.LoadFactOperator,
                operators.GapminderOperator]
    
    # Leave in for explicitness
    hooks = []
    executors = []
    #macros = [redshift_auth]
    admin_views = []
    flask_blueprints = []
    menu_links = []