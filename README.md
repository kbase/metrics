***KBase Metrics repo***

This repo contains code for gathering/loading KBase usage metrics.

**Usage**

Before being able to run this docker container
a ".env" file needs to be made. It should be called .env (nothing before the dot).
cp the example.env to .env
Then alter the file to include the correct information.

The script in hooks/build is used to build a docker image named "metrics" from the current contents of the repo. You can simply run it by
~~~
hooks/build
~~~
It fulls in import metadata about the docker image, and is used by the dockerhub and travis automated build machinery.

After the image is built, you can use the docker-compose command to run the image using the environment variables defined in the .env file mentioned above:

~~~
docker-compose run --rm metrics
~~~

That will take you into t150ca6f61fb2411e607f15fb91407a83305daaf6he docker environment (source folder of the metrics repo).
From there you can run any program in that folder.


------------------

If want to run a shell script. Here is an exmple with capturing the redirect in the environment that called Docker.

docker-compose > user_info_dump.txt run metrics ../bin/custom_scripts/dump_query_results.sh

Put executables in the bin directory we will call scripts this way for the CRON jobs
The cron jobs should run the following:
docker-compose run --rm metrics ../bin/master_cron_shell.sh

this under the hood calls
source/upload_user_stats.py
source/upload_app_stats.py
source//upload_app_category_mappings.py
source/upload_public_narratives_count.py
source/make_reporting_tables.py



--------------------
**Note on old contents**

The old contents of the metrics repo have been deleted from the current tree. The final commit containing
them was 150ca6f, if you need access to them, you can rollback your local copy of the repo to that commit,
or else just go to the following link:
   
   https://github.com/kbase/metrics/tree/150ca6f61fb2411e607f15fb91407a83305daaf6