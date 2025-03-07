# Conduit Connector Generator

The generator connector is one of [Conduit](https://github.com/ConduitIO/conduit)
builtin plugins. It generates sample records using its source connector. It has
no destination and trying to use that will result in an error.

### How to build it

Run `make`.

### Testing

Run `make test` to run all the unit tests.

### Configuration

> [!IMPORTANT]
> Parameters starting with `collections.*` are used to configure the format and
> operations for a specific collection. The `*` in the parameter name should be
> replaced with the collection name.

Below is a list of all available configuration parameters:

<!-- readmegen:source.parameters.table -->
<table class="no-margin-table">
  <tr>
    <th>Name</th>
    <th>Type</th>
    <th>Default</th>
    <th>Description</th>
  </tr>
  <tr>
<td>

`burst.generateTime`

</td>
<td>

duration

</td>
<td>

`1s`

</td>
<td>

The amount of time the generator is generating records in a burst. Has an effect only if `burst.sleepTime` is set.

</td>
  </tr>
  <tr>
<td>

`burst.sleepTime`

</td>
<td>

duration

</td>
<td>



</td>
<td>

The time the generator "sleeps" between bursts.

</td>
  </tr>
  <tr>
<td>

`collections.*.format.options.*`

</td>
<td>

string

</td>
<td>



</td>
<td>

The options for the `raw` and `structured` format types. It accepts pairs of field names and field types, where the type can be one of: `int`, `string`, `time`, `bool`, `duration`.

</td>
  </tr>
  <tr>
<td>

`collections.*.format.options.path`

</td>
<td>

string

</td>
<td>



</td>
<td>

Path to the input file (only applicable if the format type is `file`).

</td>
  </tr>
  <tr>
<td>

`collections.*.format.type`

</td>
<td>

string

</td>
<td>



</td>
<td>

The format of the generated payload data (raw, structured, file).

</td>
  </tr>
  <tr>
<td>

`collections.*.operations`

</td>
<td>

string

</td>
<td>

`create`

</td>
<td>

Comma separated list of record operations to generate. Allowed values are "create", "update", "delete", "snapshot".

</td>
  </tr>
  <tr>
<td>

`format.options.*`

</td>
<td>

string

</td>
<td>



</td>
<td>

The options for the `raw` and `structured` format types. It accepts pairs of field names and field types, where the type can be one of: `int`, `string`, `time`, `bool`, `duration`.

</td>
  </tr>
  <tr>
<td>

`format.options.path`

</td>
<td>

string

</td>
<td>



</td>
<td>

Path to the input file (only applicable if the format type is `file`).

</td>
  </tr>
  <tr>
<td>

`format.type`

</td>
<td>

string

</td>
<td>



</td>
<td>

The format of the generated payload data (raw, structured, file).

</td>
  </tr>
  <tr>
<td>

`operations`

</td>
<td>

string

</td>
<td>

`create`

</td>
<td>

Comma separated list of record operations to generate. Allowed values are "create", "update", "delete", "snapshot".

</td>
  </tr>
  <tr>
<td>

`rate`

</td>
<td>

float

</td>
<td>



</td>
<td>

The maximum rate in records per second, at which records are generated (0 means no rate limit).

</td>
  </tr>
  <tr>
<td>

`recordCount`

</td>
<td>

int

</td>
<td>



</td>
<td>

Number of records to be generated (0 means infinite).

</td>
  </tr>
</table>
<!-- /readmegen:source.parameters.table -->

### Examples

#### Bursts

The following configuration generates 100 records in bursts of 10 records each,
with a 1 second sleep time between bursts.

> [!NOTE]
> The generator currently has no concept of resuming work. For instance, below
> we have configured it to generate 100 records, but if we restart the pipeline
> (by stopping and starting the pipeline or by restarting Conduit), then it will
> start generating the 100 records from scratch.

```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        type: source
        plugin: generator
        settings:
          # global settings
          rate: 10
          recordCount: 100
          burst.generateTime: 1s
          burst.sleepTime: 1s
          # default collection
          format.type: structured
          format.options.id: int
          format.options.name: string
          operations: create
```

#### Collections

The following configuration generates records forever with a steady rate of 1000
records per second. Records are generated in the `users` and `orders` collections.
The generated records have a different format, depending on the collection they
belong to.

```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        type: source
        plugin: generator
        settings:
          # global settings
          rate: 1000
          # collection "users"
          collections.users.format.type: structured
          collections.users.format.options.id: int
          collections.users.format.options.name: string
          collections.users.operations: create
          # collection "orders"
          collections.orders.format.type: raw
          collections.orders.format.options.id: int
          collections.orders.format.options.product: string
          collections.orders.operations: create,update,delete
```

## Supported Data Types

The Generator Connector supports the following data types:

- `int`: Random integer
- `string`: Random sentence
- `time`: Current time
- `bool`: Random boolean
- `duration`: Random duration (0 to 1000 seconds)
- `name`: Random full name
- `email`: Random email address
- `employeeid`: Random employee ID (format: EMP####)
- `ssn`: Random Social Security Number (obfuscated)
- `creditcard`: Random credit card number (obfuscated)
- `ordernumber`: Random order number (format: ORD-UUID)

### New Data Types

We've added several new data types to enhance the capabilities of the Generator Connector:

1. `name`: Generates a random full name.
2. `email`: Generates a random email address.
3. `employeeid`: Generates a random employee ID in the format EMP#### (where #### is a random 4-digit number).
4. `ssn`: Generates a random Social Security Number and obfuscates it for privacy (format: XXX-XX-####).
5. `creditcard`: Generates a random credit card number and obfuscates it for privacy (format: XXXXXXXXXXXX####).
6. `ordernumber`: Generates a random order number in the format ORD-UUID.

These new types can be used in the `format.options` or `collections.*.format.options` configuration parameters, just like the existing types.

### Example Configuration with New Types

Here's an example of how to use these new types in your configuration:
