# Baltimore Re-entry Program
This project is a combination of Strapi.js Headless CMS and some python scripts for ingesting relevant data.

## System Dependencies
+ Node.JS - Latest 10.x release. I reccomend using [nvm](https://github.com/nvm-sh/nvm) to install node so you can easily switch between versions of node.
+ [Yarn Package Manager](https://yarnpkg.com/en/) - A fast javascript package manager
+ [Strapi.js](https://strapi.io/documentation/3.0.0-beta.x/getting-started/quick-start.html#_1-install-strapi-globally) - A headless CMS and admin console for managing content
+ [Python 3.7.4](https://www.python.org/downloads/)
+ [pipenv](https://github.com/pypa/pipenv)
+ [Docker](https://docs.docker.com/install/)
+ [Docker Compose](https://docs.docker.com/compose/install/)

## Local Development Startup
1. Once all system dependencies are installed you must startup mongodb as the database backing Strapi. We use docker compose to start it inside a container.
```
$ docker-compose up
```

2. Now we can startup strapi admin console and API's. We do this in watch mode so that changes automatically hot reload.
```
$ strapi develop
```
3. You can now go to the Strapi Admin console in you browser at http://localhost:1337/admin
4. You will need to create an admin user if it is you first time using that environment
5. Check out the auto-generated Swagger/OpenAPI documentation and play around with the API by going to http://localhost:1337/documentation/v1.0.0

## Ingesting Sample Data

1. Install python library dependencies
```
$ pipenv sync
```
1. Spawn a virtualenv python shell
```
$ pipenv shell
```
3. Copy the .env.example file to .env so that the environment variables are set
```
$ cp ./env.example .env
```
4. Run the ingestion script to populate a few hundred samples resources
```
$ python ./ingest/scripts/ingest_resources_csv.py
```