# Data output requirements

Platform V5 comes free of imposed data output systems compared to previous version that came bundled with Azure Data Explorer.

This change leaves an empty space in the infrastructure of the platform since data output management got removed from its internal scope.

Following this, the following requirements have been defined for a solution allowing users to easily interact with standard output systems.

## Configuration

A user needs to be able to change the output of their runner only using configuration and without having to rebundle all the solution/simulator.

### Acceptance Criteria

- Configuration changes can be applied through environment variables or configuration files
- No code recompilation or container rebuilding required for output destination changes
- Configuration validation provides clear error messages for invalid settings
- Support for multiple configuration formats (JSON, YAML, environment variables)

## Multiple output platforms

The solution to output the data must be able to send the same data into different systems with a minimal impact to the user.

### Acceptance Criteria

- Single configuration can define multiple output destinations simultaneously
- Data is successfully written to all configured platforms without duplication of effort
- Platform-specific failures do not affect other configured outputs
- Adding a new output platform requires minimal configuration changes

### Current known target platforms

PostgreSQL, Azure Data Explorer, S3 (SeaweedFS), Azure Storage

## Data access

Once the data is uploaded to a platform, the user must have ways to access that data

### Acceptance Criteria

- Standard query interfaces available for each supported platform
- Connection strings and access credentials are properly documented
- Data schema and table structures are discoverable
- Sample queries provided for common data retrieval patterns

## Filtering

As multiple runs generate different data, we need to store the data in a way that differentiates between runs

### Acceptance Criteria

- Each run has a unique identifier that persists across all output platforms
- Run metadata (timestamp, scenario ID, simulation parameters) is stored with data
- Query capabilities allow filtering by run ID, date range, and scenario parameters
- Data isolation prevents cross-run data contamination

## Initialization

In some systems, the user may need/want to initialize the system with a pre-given schema. We need to allow the user to customize and apply that initialization when needed.

### Acceptance Criteria

- Schema initialization scripts can be provided per output platform
- Initialization process is idempotent (safe to run multiple times)
- Custom schema modifications are supported without breaking core functionality
- Initialization status can be verified before data output begins

## Data cleaning

We need to have systems that allow cleaning remaining data after runs are removed from the API

### Acceptance Criteria

- Automated cleanup processes can be configured with retention policies
- Manual cleanup commands are available for specific runs or date ranges
- Cleanup operations are logged and auditable
- Data dependencies are respected during cleanup (no orphaned references)
- Cleanup confirmation prevents accidental data loss
