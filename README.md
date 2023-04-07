# Objectives
- Boot time discovery of aurora rds global cluster based on cluster id
- Regional proximity for reads and fallback to writer node if unavailable
- Healthchecks for auto discoverying topology changes

# Prequisites
- The process needs to be tied to roles that can perfrom `aws rds describe-global-cluster`

# Status
Work in porgress
