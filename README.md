# AlteryxDependencyTracker
Examine the guts (XML) of your alteryx workflows to proactively check for issues when field pruning upstream


The below is all AI written so proceed with however much value you place on that:


# Alteryx Workflow Analysis Utilities

This project provides a set of Python scripts to help download, parse, and analyze Alteryx workflows. The primary goal is to understand field usage, dependencies, and potential impact of changes within your Alteryx environment.


---

### 1. Workflow Downloader (`download_alteryx_workflows.py`)

**Purpose:**
This script connects to an Alteryx Server (Gallery) API to download specified Alteryx workflow packages (.yxzp files). It then extracts these packages into a local directory, making the .yxmd (workflow) or .yxmc (macro) files and their associated assets available for further analysis.

**Key Features:**
* Authenticates to the Alteryx Server API using OAuth2 (Client ID and Client Secret).
* Downloads one or more workflow packages based on their Workflow IDs.
* Can optionally download specific versions of a workflow.
* Extracts the downloaded .yxzp package into a designated output directory.
* Removes the temporary .yxzp file after successful extraction.

**How to Use:**
1.  **Configure Credentials:**
    * Set the `CLIENT_ID`, `CLIENT_SECRET`, and `BASE_URL` constants at the top of the script. It is highly recommended to use environment variables or a secure configuration method for credentials in a production environment rather than hardcoding them.
    * Update `WORKFLOW_OUTPUT_DIR` if you want to save workflows to a different location.
2.  **Specify Workflow IDs:**
    * In the `if __name__ == "__main__":` block, modify the `example_workflow_ids` list to include the Alteryx Server Workflow IDs you wish to download.
3.  **Run the Script:**
    ```bash
    python download_alteryx_workflows.py
    ```
    The script will create the output directory (e.g., `workflows_output`) and place the extracted workflow files within it.

**Note on SSL:** The script currently uses `verify=False` for API requests, which suppresses SSL certificate verification. For production environments, it is strongly recommended to set `use_ssl_verify = True` and ensure your system trusts the server's SSL certificate, or provide a path to a CA bundle.

---

### 2. Workflow Field Analyzer (`analyze_workflow_fields.py`)

**Purpose:**
This script analyzes a collection of local Alteryx workflow files (.yxmd, .xml – typically those downloaded by the `download_alteryx_workflows.py` script). It parses the XML structure of each workflow to identify how and where specific data fields are used across different tools.

**Key Features:**
* **Detailed Field Parsing:** Scans workflow XML to find field names within various Alteryx tool configurations (e.g., Select, Filter, Formula, Join, Sort, Summarize, Input/Output tools including Calgary and Tableau).
* **Usage Context:** Identifies the context of how a field is used (e.g., `select_input_field`, `formula_output_field`, `join_key_left`, `filter_expression_input_field`).
* **Optional Source of Truth (SoT) Lineage:**
    * If an `sot_filename_key` is provided (a string expected to be part of a Calgary tool's `RootFileName` configuration), the script traces tools downstream from these identified SoT tools.
    * The output will indicate if a field usage is `IsDownstreamSOT` (1 if downstream, 0 otherwise).
    * If no SoT key is provided, this lineage tracing is skipped, and `IsDownstreamSOT` will be 0 for all entries.
* **Usage Criticality Score:** Assigns a numerical `UsageCriticallity` score to each field usage based on the type of Alteryx tool it appears in (e.g., a Filter might be more critical than a Select tool). The mapping is defined in `TOOL_CRITICALITY_MAPPING`.
* **Last Modified Date:** Includes the `LastModified` date of each analyzed workflow file in the output.
* **Targeted Analysis:** Requires a CSV file listing specific "target field names." The script will then generate a detailed report *only* for these specified fields.
* **Processing Indicator:** Shows progress in the console as it processes workflow files.

**Input:**
* **Input Directory:** A local directory containing the Alteryx workflow files (.yxmd or .xml).
* **Target Fields CSV (Required for Output):** A CSV file where the first column lists the field names you want to analyze in detail.
* **SoT Filename Key (Optional):** A string to identify Source of Truth Calgary tools for lineage tracing.

**Output:**
* A single CSV file (default: `output_B_detailed_usage.csv`) with the following columns:
    * `FileName`: The name of the Alteryx workflow file.
    * `LastModified`: The date and time the workflow file was last modified.
    * `ToolID`: The ID of the tool within the workflow.
    * `Tool`: The plugin name of the Alteryx tool (e.g., `AlteryxBasePluginsGui.Filter.Filter`).
    * `FieldName`: The name of the data field.
    * `UsageContext`: How the field is used within the tool (e.g., `filter_expression_input_field`, `formula_output_field`).
    * `FieldUsage`: Specific details of the usage, such as the expression or configuration snippet where the field appears.
    * `IsDownstreamSOT`: `1` if the usage is downstream of an identified Source of Truth (if SoT key provided), `0` otherwise.
    * `UsageCriticallity`: A numerical score indicating the importance of the field's usage based on the tool type.

**How to Use:**
1.  **Prepare Input:**
    * Ensure your Alteryx workflow files (.yxmd or .xml) are in a single directory.
    * Create a CSV file (e.g., `fieldNames.csv`) listing the specific field names you want to track in the first column (with a header like "FieldName").
2.  **Run the Script:**
    The script uses `argparse` for command-line arguments, providing flexibility.
    ```bash
    python analyze_workflow_fields.py -i <path_to_workflows_directory> -t <path_to_target_fields.csv> -o <desired_output_filename.csv> -s <optional_sot_key>
    ```
    **Example:**
    ```bash
    python analyze_workflow_fields.py -i ./workflows_output -t ./fieldNames.csv -o ./field_impact_report.csv -s "MY_CALGARY_SOT"
    ```
    Or, to run without SoT lineage:
    ```bash
    python analyze_workflow_fields.py -i ./workflows_output -t ./fieldNames.csv
    ```
    * `-i` or `--input_dir`: Path to the directory containing workflow files.
    * `-t` or `--target_fields_csv`: Path to your CSV file listing target field names. If this file is not found or is empty, the detailed output might be empty or not generated as expected.
    * `-o` or `--output_csv`: Desired name for the output CSV report.
    * `-s` or `--sot_key`: (Optional) The Source of Truth key for lineage tracing.

**Important Considerations:**
* **Parsing Accuracy (`EnhancedNodeElement`):** The accuracy of field detection heavily depends on the parsing logic within the `EnhancedNodeElement` class in `analyze_workflow_fields.py`. This class needs to be updated/expanded if you use Alteryx tools not yet covered or if tool configurations change in different Alteryx versions.
* **Complex Tools:** Dynamic tools (like Dynamic Input, Dynamic Rename, Transpose, CrossTab) and macros can make static field tracing challenging. The script provides basic identification for some of these but may require enhancements for deep analysis.
* **SQL Parsing:** The script includes basic regex for extracting fields from SQL queries within tools like DbFileInput. For highly complex SQL, this may not be exhaustive.

---

This utility aims to provide valuable insights into your Alteryx workflows, aiding in impact analysis, dependency tracking, and overall environment management.
