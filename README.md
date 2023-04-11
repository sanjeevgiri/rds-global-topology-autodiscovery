# Objectives
- Boot time discovery of aurora rds global cluster based on cluster id
- Regional proximity for reads and fallback to primary region reader if unavailable
- Healthchecks for auto discoverying topology changes

# Prequisites
- The process needs to be tied to roles that can perfrom the following AWS API calls
  - `aws rds describe-global-cluster`
  - `aws rds describe-db-cluster-endpoints`

# Status
Work in porgress

# Notes
- AWS Toolkit plugin
- Update eclipse
- Add default in aws-vault
- Add desired profile manuall with source profile
