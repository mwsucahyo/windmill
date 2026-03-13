# Windmill Go Tools

This repository contains a collection of Go scripts designed to identify and report stock discrepancies across various systems. These scripts are intended to be executed either as [Windmill](https://www.windmill.dev/) scripts or as standalone Go applications for local testing.

## Project Structure

- **`stock-discrepancy/xmsc-xmsl`**: Tool to compare stock data between **XMS Catalyst** and **XMS Legacy** databases.
- **`stock-discrepancy/xmsl-reseller`**: Tool to compare stock data between **XMS Legacy** and the **Reseller** system.
- **`stock-discrepancy/xmsl-uf`**: Tool for comparison between **XMS Legacy** and **UF** (Under development).

## Features

- **Automated Comparison**: Queries multiple databases/systems to identify mismatches in `qty_available`.
- **Windmill Native**: Designed to work seamlessly with Windmill resources for database credentials.
- **Rich Reporting**: Generates Mattermost-compatible Markdown tables for alerting.
- **Local Development**: Includes support for `.env` files to facilitate local testing without deploying to Windmill.

## Getting Started

### Prerequisites

- [Go](https://go.dev/dl/) (v1.23+ recommended)
- Access to the relevant PostgreSQL databases.

### Local Setup

1. **Environment Variables**:
   Copy the `.env.example` file to `.env` and fill in your database connection details (DSNs).

   ```bash
   cp .env.example .env
   ```

2. **Run a Script Locally**:
   Execute the tool using the `cmd` subfolder:
   ```bash
   # Compare Catalyst and Legacy
   go run ./stock-discrepancy/xmsc-xmsl/cmd

   # Compare Reseller and Legacy
   go run ./stock-discrepancy/xmsl-reseller/cmd
   ```

## Windmill Integration

Each script typically exports a `Main` function that Windmill calls as the script entry point. Ensure that the necessary Windmill resources (e.g., PostgreSQL DSN resources) are available and correctly named in your Windmill workspace as referenced in the code.

## License

[MIT](LICENSE) (or specify your license here)
