# orrp Query Language

orrp provides a SQL-like query language for writing and reading events. This document covers the syntax and features.

## Events

An event is the fundamental unit of data in orrp. Each event is associated with an entity and contains arbitrary key-value tags.

### Creating Events

Use the `EVENT` command to write data:

```
EVENT in:<namespace> entity:<entity_id> <tag1>:<value1> <tag2>:<value2> ...
```

**Parameters:**
- `in:<namespace>` - Required. The database namespace to write to
- `entity:<entity_id>` - Required. The entity this event belongs to (e.g., user ID, device ID, service name)
- `<key>:<value>` - Optional. Arbitrary key-value pairs to tag the event

**Examples:**

```
EVENT in:analytics_02_10_2026 entity:user123 action:purchase amount:99.99 currency:USD

EVENT in:audit entity:service-api endpoint:/login attempt:failed status:401

EVENT in:iot entity:sensor_42 temperature:42.5 humidity:65.2 location:warehouse_a
```

**Notes:**
- Each event receives an auto-generated ID and timestamp (`ts`)
- Events are **immutable** - they cannot be modified or deleted after creation

## Querying Events

Use the `QUERY` command to retrieve events:

```
QUERY in:<namespace> [options] where:<filter_expression>
```

**Options:**
- `in:<namespace>` - Required. The namespace to query
- `where:<filter_expression>` - Required. Filter events by tag values and timestamps (see [Filtering](#filtering) below)
- `take:<limit>` - Optional. Limit the number of results (default: all matching events)
- `cursor:<event_id>` - Optional. Start results from a specific event ID for pagination

### Filtering

Use the `where` clause to filter events based on tag values and timestamps. Filters use prefix notation with AND, OR, and NOT operators.

#### Basic Filters

Match events where a tag has a specific value:

```
QUERY in:analytics where:(action:purchase)
```

Returns all events in the `analytics` namespace where the `action` tag equals `purchase`.

#### AND Logic

Match events that satisfy multiple conditions:

```
QUERY in:analytics where:(action:purchase AND amount:99.99 AND location:california)
```

Returns events where `action` is `purchase` **AND** `amount` is `99.99` **AND** `location` is `california`.

#### OR Logic

Match events that satisfy any condition:

```
QUERY in:analytics where:(country:US OR country:CA OR country:MX)
```

Returns events where the `country` tag is one of: US, CA, or MX.

#### NOT Logic

Exclude events matching a condition:

```
QUERY in:analytics where:(NOT status:failed)
```

Returns all events where the `status` tag is not `failed`.

```
QUERY in:analytics where:(action:login AND NOT country:blocked)
```

Returns login events from countries that are not blocked.

#### Nested Expressions

Parentheses control evaluation order:

```
QUERY in:analytics where:((action:login AND country:US) OR (action:signup AND verified:true))
```

Returns events that either:
- Have `action` = `login` **AND** `country` = `US`, or
- Have `action` = `signup` **AND** `verified` = `true`

**Operator Precedence** (highest to lowest):
1. NOT
2. Comparison operators (=)
3. AND
4. OR

#### Timestamp Ranges

Filter events by time using comparison operators on the `ts` (timestamp) field. Timestamps are in **milliseconds**:

```
QUERY in:analytics where:(ts > 1704067200000)
```

Returns all events with a timestamp after that date.

**Supported Operators:**
- `ts > <milliseconds>` - Greater than
- `ts < <milliseconds>` - Less than
- `ts >= <milliseconds>` - Greater than or equal
- `ts <= <milliseconds>` - Less than or equal

**Combined with Tag Filtering:**

```
QUERY in:analytics where:(action:purchase AND ts > 1704067200000)
```

Returns purchase events from a specific date onward.

```
QUERY in:analytics where:(ts > 1704067200000 AND ts < 1704153600000)
```

Returns all events within a time window.

### Pagination

Retrieve results in pages using cursors:

```
QUERY in:analytics where:(country:US) take:100
```

Returns the first 100 results.

```
QUERY in:analytics where:(country:US) take:100 cursor:5042
```

Returns 100 results starting from event ID `5042`, useful for fetching the next page.

**How Pagination Works:**

1. Make an initial query with `take:<limit>`:
   ```
   QUERY in:analytics where:(action:purchase) take:50
   ```

2. If there are more results, the response includes a `next_cursor` field with the event ID to use for the next query.

3. Fetch the next page:
   ```
   QUERY in:analytics where:(action:purchase) take:50 cursor:<next_cursor_value>
   ```

4. Results are ordered by event ID (creation order).

**Parameters:**
- `take:<number>` - Limit results to this many events
- `cursor:<event_id>` - Start from this event ID (exclusive; results start *after* this ID)

### Limiting Results

The `take` parameter limits the number of results returned:

```
QUERY in:analytics where:(country:US) take:10
```

Returns at most 10 events matching the filter.

```
QUERY in:analytics where:(action:purchase) take:1000
```

Returns at most 1000 events. Useful for batch processing without pagination.

## Query Response Format

Queries return a msgpack response of event objects. Each event contains:

- `id` - Unique event identifier (auto-generated)
- `ts` - Event timestamp in milliseconds (Unix epoch)
- **Tag fields** - All key-value pairs you created with the event

**Example Response:**

```json
{
  "data": {
    "objects": [
      {
        "entity": "abc",
        "id": 1,
        "in": "mtest",
        "ts": 1772174019741
      },
      {
        "entity": "abc",
        "id": 2,
        "in": "mtest",
        "loc": "ca",
        "ts": 1772174040861
      },
      {
        "entity": "abc",
        "id": 3,
        "in": "mtest",
        "loc": "ca",
        "ts": 1772174088052
      }
    ]
  },
  "status": "OK"
}
```

## Full Examples

### Example 1: E-Commerce Analytics

```
# Log a purchase event
EVENT in:orders entity:user_123 action:purchase product:laptop amount:999.99 country:US payment:credit_card

EVENT in:orders entity:user_456 action:purchase product:mouse amount:29.99 country:CA payment:debit_card

EVENT in:orders entity:user_789 action:cart_view product:keyboard country:US

# Query for high-value purchases
QUERY in:orders where:(action:purchase AND amount:999.99)

# Query for purchases in North America
QUERY in:orders where:((country:US OR country:CA) AND action:purchase) take:100

# Query for failed payments
QUERY in:orders where:(action:purchase AND NOT payment:credit_card)
```

### Example 2: IoT Sensor Monitoring

```
# Log sensor readings
EVENT in:sensors entity:sensor_floor1_temp temperature:22.5 humidity:45.2 status:ok
EVENT in:sensors entity:sensor_floor2_temp temperature:28.9 humidity:62.1 status:warning
EVENT in:sensors entity:sensor_floor1_temp temperature:23.2 humidity:44.8 status:ok

# Find all warning sensors
QUERY in:sensors where:(status:warning)

# Find readings from the past hour
QUERY in:sensors where:(ts > 1704063234567)

# Find normal readings from a specific sensor entity
QUERY in:sensors where:(entity:sensor_floor1_temp AND status:ok)
```

### Example 3: Application Audit Logs

```
# Log authentication events
EVENT in:audit entity:app-server action:login user:alice result:success ip:192.168.1.100
EVENT in:audit entity:app-server action:login user:bob result:failed ip:203.0.113.45
EVENT in:audit entity:app-server action:login user:alice result:success ip:192.168.1.100
EVENT in:audit entity:app-server action:logout user:alice

# Find failed login attempts
QUERY in:audit where:(action:login AND result:failed)

# Find audit events from internal IPs
QUERY in:audit where:((ip:192.168.1.100 OR ip:10.0.0.0) AND action:login)

# Paginate through login events
QUERY in:audit where:(action:login) take:50
QUERY in:audit where:(action:login) take:50 cursor:<next_cursor>

# Find sensitive events (logins or data access)
QUERY in:audit where:((action:login OR action:data_access) AND NOT result:failed)
```

## Query Syntax Summary

| Operation | Syntax | Example |
|-----------|--------|---------|
| Write Event | `EVENT in:<ns> entity:<id> <k>:<v> ...` | `EVENT in:orders entity:u1 action:purchase` |
| Basic Query | `QUERY in:<ns> where:(<tag>:<value>)` | `QUERY in:orders where:(action:purchase)` |
| AND | `QUERY in:<ns> where:(<cond1> AND <cond2>)` | `QUERY in:orders where:(action:purchase AND amount:99.99)` |
| OR | `QUERY in:<ns> where:(<cond1> OR <cond2>)` | `QUERY in:orders where:(country:US OR country:CA)` |
| NOT | `QUERY in:<ns> where:(NOT <condition>)` | `QUERY in:orders where:(NOT status:failed)` |
| Nested | `QUERY in:<ns> where:((<cond1> AND <cond2>) OR <cond3>)` | `QUERY in:orders where:((action:purchase AND amount>50) OR status:pending)` |
| Timestamp | `QUERY in:<ns> where:(ts > <ms>)` | `QUERY in:orders where:(ts > 1704067200000)` |
| Limit | `QUERY in:<ns> take:<count> where:(<condition>)` | `QUERY in:orders take:100 where:(action:purchase)` |
| Pagination | `QUERY in:<ns> cursor:<id> where:(<condition>)` | `QUERY in:orders cursor:5042 where:(action:purchase)` |
