# Conflixis Engine
Conflixis Monorepo

## Monorepo Overview
This repository hosts several services that together form the Conflixis platform.
Each package in `./packages` is a deployable service or shared library.

- **api** – Express based backend providing the REST API and orchestrating long
  running jobs.
- **jobs** – Collection of Google Cloud Run jobs for heavy data processing.
- **client** – *Deprecated* marketing site. Functionality will be migrated into the `manager` package.
- **portal** – Authenticated Next.js portal for customers.
- **manager** – Client-facing application where organizations manage data and publish disclosure campaigns.
- **core** – Shared TypeScript utilities such as the Job Manager library.
- **ui** – Reusable React component library shared across the frontends.
- **postgrest** – Container wrapper that exposes the Postgres database via
  PostgREST in development.

Local development typically involves running several of these services together
via `docker-compose`.

## Dependencies
The services run on **Node.js** with web frontends built on **Next.js**.
Data is stored in **PostgreSQL** and **Firestore**. Jobs and the API deploy to
**Google Cloud Run** while the web apps deploy to **Vercel**. Development uses
**Docker Compose** with `docker-compose.yml` to orchestrate the stack.

## Services
- **api**: The main API for the engine. This is a node express app that serves as the main entrypoint for the engine. It handles light data processing and job routing. IT also serves as the main API for the web app. It is deployed as a Cloud Run service.

The API has three environments:
  - **dev**: Runs locally at `localhost:3000`. Works for local development in conjunction with the `web` service. This works for most development purposes but does not play nicely with the `jobs` service due to it's ephemeral nature. See more about this in the Deployments section below. Defaults to the `conflixis-web-stage` Firebase project
  - **stage**: `https://api-node-stage-afkcxqwkva-uc.a.run.app`. This is the staging environment. It is deployed to the `conflixis-web-stage` Firebase project
  - **prod**: `https://api-node-afkcxqwkva-uc.a.run.app`. This is the production environment. It is deployed to the `conflixis-web` Firebase project
- **client**: *Deprecated* Next.js site currently hosted at `client.conflixis.com`. Features are moving to `manager`.
- **portal**: Next.js portal at `portal.conflixis.com`
- **manager**: Client-facing application for managing group data and publishing disclosure campaigns.
- **jobs**: A collection of Cloud Run Jobs that handle heavy data processing. These are long running jobs that may take 5-30 minutes to complete. They are deployed as Cloud Run services. See more about jobs in the [readme](./jobs/README.md). There is a **stage** & **prod** environment for each job. The stage environment is built in as `Jobs-stage`, and prod is `Jobs`.

# Getting Started
See the [documentation on Notion](https://www.notion.so/Getting-Started-197985870a2d801da430e62477622804?pvs=4)

### Local development
Install dependencies and bring up the stack with Docker Compose:

```bash
npm install
npm run dev
```

This will start the API, web frontends and supporting services defined in
`docker-compose.yml`. Environment variables for each service can be configured
in the corresponding `.env.local` files under each package.

## Deploy
"MAIN is golden!" The services will all deploy automatically when merged to `main`:
- `web` will deploy feature branches and prod automatically via Vercel
- All other services automatically build via Google Cloud Build and deploy via Google Cloud Run

### Deployments
#### Prod
Production deployments are triggered by merging to `main` branch. Only services touched in a branch will be built and deployed. For example, if you only touch `api-node` in a branch, only `api-node` will be built and deployed. If you touch `api-node` and `jobs`, both will be built and deployed.

- `web` is deployed to `client.conflixis.com` by Vercel.
- `api-node` is deployed to `https://api-node-afkcxqwkva-uc.a.run.app`.
- `jobs` images are set to `latest`.

All prod services are set up to use the `conflixis-web` Firebase project.

#### Stage
Stage deployments are triggered by pushing to any branch that's not `prod`. Only services touched in a branch will be built and deployed. For example, if you only touch `api-node` in a branch, only `api-node` will be built and deployed. If you touch `api-node` and `jobs`, both will be built and deployed.

- `web` is deployed to various feature deployments by Vercel. Currently these deployments are not used and auth is not set up for them.
- `api-node` is deployed to `https://api-node-stage-afkcxqwkva-uc.a.run.app`. `Dev` services will utilize this environment as needed (e.g. pub/sub).
- `jobs` images are set to `stage`.

All stage services are set up to use the `conflixis-web-stage` Firebase project.

#### Dev
Dev deployments run locally via Docker Compose. See the [Develop](#develop) section above for more info. This environment will use `stage` services as needed (e.g. jobs, pub/sub).


### Pub/Sub
Pub/Sub is a messaging service that allows us to coordinate async workflows. It's primary use is to coordinate the `jobs` service. It is also used to coordinate the `api-node` service with the `jobs` service via Orchestrators (see below).


### Orchestrators
Orchestrators are a tool we use to coordinate complex async workflows. When a topic is published to, the appropriate orchestrator will recieve a push notification and kick off the workflow. Subsequent calls to the orchestrator, along with additional information will manage the workflow.

An orchestrator is a stateless api endpoint that can reference data in appropriate Firestore documents to manage the workflow. It will kick off jobs and listen for pub/sub messages to know when to kick off the next job in the workflow. It will also listen for messages from the jobs to know when the workflow is complete.


#### Group Data Processor
Responsible for taking a group and processing all of its suppliers and NPIs. This is a long running job that may take 5-30 minutes to complete depending on the group size. It will publish a message to the topic when it is finished.

- Depends on a `group` document in Firestore complete with NPIs (suppliers are optional)


## Documentation
API docs can be found [here](./docs/index.html). Open this in a browser window!

To update the docs, run the following command from the root folder:
```bash
npm run document
```

Documentation made possible by [swagger-autogen](https://github.com/swagger-autogen/swagger-autogen) & [redocly](https://redocly.com/docs/cli/).

- TODO: automate this process with a pre-commit hook

## SBOM
Software Bill of Materials (SBOM or BOM) is a way to ID & share all components that go into the product. In order to generate these for the project do the following command to rebuild and output the manifests to the root directory:

```bash
npm run gen-bom
```

There are several formats, but we're using [cyclonedx](https://github.com/CycloneDX/cyclonedx-node-npm) for node

## Architecture
![High level architecture diagram](docs/resources/architecture-diagram.png)
([diagram](https://lucid.app/lucidchart/bf1c94c6-2a94-4a03-a77b-3df018c97087/edit?invitationId=inv_cebb1e3d-e581-413d-a99d-ac71184ce368&page=0_0#))

## Backups
🚨 For restoration process see [BACKUP_RESTORE.md](./BACKUP_RESTORE.md)

Firebase Firestore Database [instructions](https://firebase.google.com/docs/firestore/backups)

```bash
# list projects
gcloud projects list

# create daily schedule
gcloud alpha firestore backups schedules create \
--database='(default)' \
--recurrence=daily \
--retention=7d
```

```bash
# List active backup schedules
gcloud alpha firestore backups schedules list \
--database='(default)'
```

```bash
# Describe backup schedule. get id from list above
gcloud alpha firestore backups schedules describe \
--database='(default)' \
--backup-schedule=BACKUP_SCHEDULE_ID
```

```bash
# List backups
gcloud alpha firestore backups list \
--format="table(name, database, state)"
```

```bash
# restore
gcloud alpha firestore databases restore \
--source-backup=projects/PROJECT_ID/locations/LOCATION/backups/BACKUP_ID \
--destination-database='(default)'
```

## Load Balancer
How to: https://cloud.google.com/load-balancing/docs/l7-internal/setting-up-l7-internal-serverless#console
Create SSL: https://cloud.google.com/load-balancing/docs/ssl-certificates/self-managed-certs

## Create Elastic API key
do this in the EC console

Creates a new API key
```
POST /_security/api_key
{
  "name": "engine-reader",
  "role_descriptors": { 
    "read_access": { 
      "cluster": ["all"],
      "index": [
        {
          "names": ["op-general", "op-research", "op-ownership", "npi"],
          "privileges": ["read"]
        }
      ]
    }
  },
  "metadata": {
    "app": "conflixis_engine"
  }
}
```


## License
This repo is All Rights Reserved
