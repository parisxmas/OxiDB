# Cron Scheduler

OxiDB includes a built-in scheduler that runs [stored procedures](stored-procedures.md) on a recurring schedule. Schedules can use cron expressions for precise timing or simple interval strings for periodic execution.

## Concept

A schedule defines:
- **name**: Unique identifier
- **procedure**: Name of the stored procedure to execute
- **cron** or **every**: When to run (one is required)
- **params**: Parameters to pass to the procedure (optional)
- **enabled**: Whether the schedule is active (default: `true`)

The scheduler runs as a background thread, checking for due schedules every second. Each execution calls the named procedure with the specified parameters.

## Creating a Schedule

### With Cron Expression

```json
{
  "command": "create_schedule",
  "name": "nightly_cleanup",
  "procedure": "cleanup_expired",
  "cron": "0 2 * * *",
  "params": {"days_old": 30},
  "enabled": true
}
```

### With Interval

```json
{
  "command": "create_schedule",
  "name": "health_check",
  "procedure": "check_system_health",
  "every": "5m",
  "enabled": true
}
```

## Cron Expressions

Standard 5-field cron format:

```
minute  hour  day-of-month  month  day-of-week
  *       *        *          *        *
```

| Field | Allowed Values | Special Characters |
|-------|---------------|-------------------|
| Minute | 0-59 | `*`, `,`, `-`, `/` |
| Hour | 0-23 | `*`, `,`, `-`, `/` |
| Day of month | 1-31 | `*`, `,`, `-`, `/` |
| Month | 1-12 | `*`, `,`, `-`, `/` |
| Day of week | 0-6 (0 = Sunday) | `*`, `,`, `-`, `/` |

### Examples

| Expression | Meaning |
|-----------|---------|
| `* * * * *` | Every minute |
| `0 * * * *` | Every hour |
| `0 2 * * *` | Daily at 2:00 AM |
| `0 0 * * 0` | Weekly on Sunday at midnight |
| `0 0 1 * *` | First day of every month |
| `*/5 * * * *` | Every 5 minutes |
| `0 9-17 * * 1-5` | Every hour from 9 AM to 5 PM, Monday to Friday |
| `0,30 * * * *` | Every 30 minutes (at :00 and :30) |

### Special Characters

- `*` -- matches all values
- `,` -- list separator (`1,15` means the 1st and 15th)
- `-` -- range (`9-17` means 9 through 17)
- `/` -- step (`*/5` means every 5 units)

## Interval Strings

Simple interval syntax as an alternative to cron:

| Format | Example | Meaning |
|--------|---------|---------|
| `Ns` | `30s` | Every 30 seconds |
| `Nm` | `5m` | Every 5 minutes |
| `Nh` | `2h` | Every 2 hours |

## Managing Schedules

### List Schedules

```json
{"command": "list_schedules"}
```

### Get Schedule

```json
{"command": "get_schedule", "name": "nightly_cleanup"}
```

Response includes execution state:

```json
{
  "ok": true,
  "data": {
    "name": "nightly_cleanup",
    "procedure": "cleanup_expired",
    "cron": "0 2 * * *",
    "params": {"days_old": 30},
    "enabled": true,
    "last_run": "2025-03-15T02:00:00Z",
    "last_status": "ok",
    "last_error": null,
    "run_count": 42
  }
}
```

### Delete Schedule

```json
{"command": "delete_schedule", "name": "nightly_cleanup"}
```

### Enable / Disable Schedule

Pause a schedule without deleting it:

```json
{"command": "disable_schedule", "name": "nightly_cleanup"}
```

Resume:

```json
{"command": "enable_schedule", "name": "nightly_cleanup"}
```

## State Tracking

Each schedule tracks its execution history:

| Field | Description |
|-------|-------------|
| `last_run` | Timestamp of the last execution |
| `last_status` | `"ok"` or `"error"` |
| `last_error` | Error message if the last run failed |
| `run_count` | Total number of executions |

## Client Examples

### Python

```python
# Create with cron
client.create_schedule("nightly_cleanup", "cleanup_expired",
                       cron="0 2 * * *", params={"days_old": 30})

# Create with interval
client.create_schedule("health_check", "check_system_health", every="5m")

# List schedules
schedules = client.list_schedules()

# Get schedule details
schedule = client.get_schedule("nightly_cleanup")

# Disable / enable
client.disable_schedule("nightly_cleanup")
client.enable_schedule("nightly_cleanup")

# Delete
client.delete_schedule("nightly_cleanup")
```

### Go

```go
// Create with cron
client.CreateSchedule("nightly_cleanup", "cleanup_expired", map[string]any{
    "cron":   "0 2 * * *",
    "params": map[string]any{"days_old": 30},
})

// Create with interval
client.CreateSchedule("health_check", "check_system_health", map[string]any{
    "every": "5m",
})

// List / get
schedules, _ := client.ListSchedules()
schedule, _ := client.GetSchedule("nightly_cleanup")

// Disable / enable
client.DisableSchedule("nightly_cleanup")
client.EnableSchedule("nightly_cleanup")

// Delete
client.DeleteSchedule("nightly_cleanup")
```

### Java

```java
// Create with cron
db.createSchedule("nightly_cleanup", "cleanup_expired", "0 2 * * *",
    Map.of("days_old", 30), true);

// Create with interval
db.createScheduleInterval("health_check", "check_system_health", "5m");

// List / get
JsonNode schedules = db.listSchedules();
JsonNode schedule = db.getSchedule("nightly_cleanup");

// Disable / enable
db.disableSchedule("nightly_cleanup");
db.enableSchedule("nightly_cleanup");

// Delete
db.deleteSchedule("nightly_cleanup");
```

### Julia

```julia
# Create with cron
create_schedule(client, "nightly_cleanup", "cleanup_expired";
                cron="0 2 * * *", params=Dict("days_old" => 30))

# Create with interval
create_schedule(client, "health_check", "check_system_health"; every="5m")

# List / get
schedules = list_schedules(client)
schedule = get_schedule(client, "nightly_cleanup")

# Disable / enable
disable_schedule(client, "nightly_cleanup")
enable_schedule(client, "nightly_cleanup")

# Delete
delete_schedule(client, "nightly_cleanup")
```

### .NET

```csharp
// Create schedule
db.CreateSchedule("""{"name": "nightly_cleanup", "procedure": "cleanup_expired", "cron": "0 2 * * *", "params": {"days_old": 30}}""");

// List / get
var schedules = db.ListSchedules();
var schedule = db.GetSchedule("nightly_cleanup");

// Disable / enable
db.DisableSchedule("nightly_cleanup");
db.EnableSchedule("nightly_cleanup");

// Delete
db.DeleteSchedule("nightly_cleanup");
```

### Swift

```swift
// Create schedule
try db.createSchedule(definition: [
    "name": "nightly_cleanup",
    "procedure": "cleanup_expired",
    "cron": "0 2 * * *",
    "params": ["days_old": 30]
])

// List / get
let schedules = try db.listSchedules()
let schedule = try db.getSchedule(name: "nightly_cleanup")

// Disable / enable
try db.disableSchedule(name: "nightly_cleanup")
try db.enableSchedule(name: "nightly_cleanup")

// Delete
try db.deleteSchedule(name: "nightly_cleanup")
```

## See Also

- [Stored Procedures](stored-procedures.md) -- define the procedures that schedules execute
- [Server Configuration](server.md) -- server must be running for the scheduler to operate
