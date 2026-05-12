# IICPC Summer Hackathon 2026

## Prerequisites

### Dev Environment

You need to have docker and docker-buildx installed.

Run the script `./scripts/setup-host.sh` to install the prerequisites.
Afterwards, run `just`.

## The direction for the project

The technical demand was a system where multiple services work in parallel
and yet have to communicate and work with each other to accept submissions,
build the project, run the binary, test it via a high number of bots,
then log the telemetry data and use it for scoring a user's submission.

The project also required to be easily deployed to a cloud environment with
automatic provisioning and high scalability. For me, this meant that the system
can naturally be decomposed into a microservice architecture written in Go.
Knowing that I needed to *orchestrate* a lot of microservices in a highly
scalable manner, I picked Kubernetes with k3s, a lightweight runtime.

My initial thought was to run it inside a virtual machine to keep my dev
environment clean and also make it easily reproducible, but the closest
option matching what I wanted (Lima - Linux Virtual Machines) was very
finicky, so I decided to just write scripts to automate dependency download
and bringing up the system. This is where k3s shone, being just a single
command for installation.

gVisor was chosen to have a highly secure runtime environment. The final
binary is executed in a sandbox environment with the gVisor runtime.
The build pod for the binary itself uses the default runc runtime, but
without any internet access via egress policies. I accept the risk that
might come from not using gVisor for the build pods since I prefer having
well-vetted compiler images and seamless builds over protection at build-time.

Helm has been included for simplifying package management and dependencies in
the future, to prepare for cloud deployment readiness. Just is being used to
easily write scripts for automation at the development environment level.

## Meet the services

Let's talk about the services. First, the ones that I'm writing myself:

- submission-api
- build-service
- sandbox-orchestrator
- bot-orchestrator
- bot-runner
- telemetry-ingester

These are all pretty self explanatory, and have separate documentation.

Next come the services being used for my infrastructure:

### Redpanda

This was used to orchestrate the microservices with an event-drive approach.
I didn't pick Kafka here because Redpanda is easier to deploy and has higher
performance. Kafka might be more appealing because it has been "battle-tested"
and is more established as the standard to reach for, I think these 2 advantages
were enough to pick Redpanda over Kafka.
