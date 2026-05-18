# k0s + Cilium Ansible Playbook

Installs k0s (single-node, expandable to multi-node) with Cilium CNI and
kube-proxy replacement. Tested on Arch Linux (bare metal) and designed to
work on AWS (Ubuntu 22.04+, AL2023).

## Structure

```
.
├── site.yml              # Main playbook
├── inventory.ini         # Target hosts — edit before running
└── group_vars/
    └── all.yml           # Variables — review before running
```

## Before running

**1. Edit `inventory.ini`**

Local dev (Arch):

```ini
[k0s_controller]
localhost ansible_connection=local
```

AWS (cloud):

```ini
[k0s_controller]
1.2.3.4 ansible_user=ubuntu ansible_ssh_private_key_file=~/.ssh/your-key.pem
```

**2. Review `group_vars/all.yml`**

Key values to confirm:

- `pod_cidr` — must not overlap with your node network or VPC CIDR
- `service_cidr` — must not overlap with your VPC CIDR
- `cilium_version` — pin this for reproducibility
- `helm_version` — pin this for reproducibility

## Running

```bash
# Dry run first
ansible-playbook -i inventory.ini site.yml --check

# Full run
ansible-playbook -i inventory.ini site.yml
```

## Notes

- `--enable-worker --no-taints` is used instead of `--single` so the cluster
  can be expanded to multi-node later without reinstalling.
- Pod CIDR `172.16.0.0/16` is chosen to avoid conflict with AWS VPC
  `10.0.0.0/16` and typical home/lab `192.168.x.x` ranges.
- Cilium uses VXLAN tunnel mode for cloud agnosticism — works on bare metal
  and AWS without VPC route table changes or ENI mode.
- The playbook is idempotent — safe to re-run.
