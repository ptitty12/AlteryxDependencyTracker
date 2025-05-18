import xml.etree.ElementTree as ET
import csv
from shutil import copyfile
import sys
import os
import re
from collections import deque, defaultdict
import datetime # Added for LastModified date

# --- Tool Criticality Mapping ---
TOOL_CRITICALITY_MAPPING = {
    "AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect": 0,
    "AlteryxBasePluginsGui.Filter.Filter": 5,
    "AlteryxBasePluginsGui.Formula.Formula": 5,
    "AlteryxBasePluginsGui.Join.Join": 5,
    "AlteryxBasePluginsGui.Sort.Sort": 2,
    "AlteryxSpatialPluginsGui.Summarize.Summarize": 1,
    "AlteryxBasePluginsGui.SummarizeConfigurable.SummarizeConfigurable": 1,
    "CalgaryLoadersGui.CalgaryLoader.CalgaryLoader": 2,
    "CalgaryPluginsGui.CalgaryInput.CalgaryInput": 5,
    "CalgaryPluginsGui.CalgaryJoin.CalgaryJoin": 5,
    "TableauOutput_1_3_1": 4,
    "TableauOutput_1_4_0": 4
}

class EnhancedNodeElement(object):
    def __init__(self, node_xml):
        self.tool_id = node_xml.attrib.get('ToolID', 'UnknownToolID')
        self.plugin = None
        self.node_xml = node_xml

        gui_settings_node = node_xml.find('GuiSettings')
        if gui_settings_node is not None:
            self.plugin = gui_settings_node.attrib.get('Plugin')

        self.extracted_fields = []
        self.calgary_root_filename = None
        self._parse_configuration()

    def _add_field(self, name, context, detail, is_output=False):
        if name:
            self.extracted_fields.append({
                "field_name": name,
                "usage_context": context,
                "detail": detail,
                "is_output": is_output
            })

    def _parse_expression_for_fields(self, expression, base_context, detail_for_extraction, is_output_for_named_field=False, output_field_name=None):
        if not expression:
            return
        if output_field_name:
            self._add_field(output_field_name, f"{base_context}_output", detail_for_extraction, is_output=True)

        found_fields = re.findall(r'\[([^\]]+)\]', expression)
        for field in found_fields:
            self._add_field(field, f"{base_context}_input", detail_for_extraction, is_output=False)

    def _parse_configuration(self):
        if self.node_xml is None:
            return
        try:
            properties_node = self.node_xml.find('Properties')
            if properties_node is None: return
            configuration_node = properties_node.find('Configuration')
            if configuration_node is None: return

            # --- AlteryxSelect ---
            if self.plugin == 'AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect' or self.plugin == 'AlteryxBasePluginsGui.MultiFieldSelect.MultiFieldSelect':
                select_fields_node = configuration_node.find('SelectFields')
                if select_fields_node is not None:
                    for field_node in select_fields_node.findall('SelectField'):
                        field_name = field_node.get('field')
                        renamed_to = field_node.get('rename')
                        is_selected = field_node.get('selected') == 'True'
                        if is_selected and field_name:
                            self._add_field(field_name, "select_input_field", f"Selected, renamed to: {renamed_to if renamed_to else 'N/A'}", is_output=False)
                            if renamed_to and renamed_to != field_name:
                                self._add_field(renamed_to, "select_output_renamed_field", f"Renamed from: {field_name}", is_output=True)
                            elif not renamed_to :
                                self._add_field(field_name, "select_output_passthrough_field", "Selected, not renamed", is_output=True)
                dynamic_unknown_node = configuration_node.find('SelectConfiguration')
                if dynamic_unknown_node is not None and dynamic_unknown_node.get('DeselectUnknown') == 'False':
                       self._add_field("*UnknownOrDynamicFields*", "select_dynamic_passthrough", "Dynamic/Unknown fields are passed through", is_output=True)

            # --- Join Tool ---
            elif self.plugin == 'AlteryxBasePluginsGui.Join.Join':
                for join_info_node in configuration_node.findall('JoinInfo'):
                    connection_side = join_info_node.get('connection')
                    for field_node in join_info_node.findall('Field'):
                        field_name = field_node.get('field')
                        if field_name:
                            self._add_field(field_name, f"join_key_{connection_side.lower()}", f"Join key on {connection_side}", is_output=False)
                            self._add_field(field_name, f"join_output_from_{connection_side.lower()}", f"Output from {connection_side} join key", is_output=True)

            # --- Filter Tool ---
            elif self.plugin == 'AlteryxBasePluginsGui.Filter.Filter':
                expression_node = configuration_node.find('Expression')
                if expression_node is not None and expression_node.text:
                    self._parse_expression_for_fields(expression_node.text, "filter_expression", expression_node.text)

            # --- Formula Tool ---
            elif self.plugin == 'AlteryxBasePluginsGui.Formula.Formula':
                formula_fields_container = configuration_node.find('FormulaFields')
                if formula_fields_container is not None:
                    for ff_node in formula_fields_container.findall('FormulaField'):
                        output_field = ff_node.get('field')
                        expression = ff_node.get('expression')
                        self._add_field(output_field, "formula_output_field", expression, is_output=True)
                        if expression:
                            self._parse_expression_for_fields(expression, "formula_expression", expression)

            # --- Summarize Tool ---
            elif self.plugin == 'AlteryxSpatialPluginsGui.Summarize.Summarize' or self.plugin == 'AlteryxBasePluginsGui.SummarizeConfigurable.SummarizeConfigurable':
                summarize_fields_node = configuration_node.find('SummarizeFields')
                if summarize_fields_node is not None:
                    for sf_node in summarize_fields_node.findall('SummarizeField'):
                        field_name = sf_node.get('field')
                        action = sf_node.get('action')
                        output_rename = sf_node.get('rename')
                        if field_name:
                               self._add_field(field_name, f"summarize_input_field_for_{action}", f"Action: {action}, Output: {output_rename if output_rename else field_name}", is_output=False)
                        if output_rename:
                               self._add_field(output_rename, f"summarize_output_field_from_{action}", f"Original: {field_name}, Action: {action}", is_output=True)
                        elif action and "GroupBy" in action and field_name:
                               self._add_field(field_name, f"summarize_output_field_from_{action}", f"Original: {field_name}, Action: {action}", is_output=True)

            # --- Sort Tool ---
            elif self.plugin == 'AlteryxBasePluginsGui.Sort.Sort':
                sort_info_node = configuration_node.find('SortInfo')
                if sort_info_node is not None:
                    for field_node in sort_info_node.findall('Field'):
                        field_name = field_node.get('field')
                        self._add_field(field_name, "sort_key_field", f"Order: {field_node.get('order', 'Ascending')}", is_output=False)
                        self._add_field(field_name, "sort_output_passthrough_field", "Field used for sorting (passes through)", is_output=True)

            # --- DbFileInput Tool ---
            elif self.plugin == 'AlteryxBasePluginsGui.DbFileInput.DbFileInput':
                file_node = configuration_node.find('File')
                query_node = configuration_node.find('Query')
                sql_query = None
                if query_node is not None and query_node.text:
                    sql_query = query_node.text.strip()
                elif file_node is not None and file_node.text:
                    parts = file_node.text.split('|||')
                    if len(parts) > 1 and any(kw in parts[1].lower() for kw in ["select ", " from ", " where "]):
                        sql_query = parts[1].strip()
                    elif "select " in file_node.text.lower():
                        sql_query = file_node.text.strip()
                if sql_query:
                    fields_in_select = re.findall(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
                    if fields_in_select:
                        potential_fields_str = fields_in_select[0]
                        for pf_with_alias in potential_fields_str.split(','):
                            pf_match = re.match(r'(\S+)\s+AS\s+(\S+)', pf_with_alias.strip(), re.IGNORECASE)
                            if pf_match:
                                self._add_field(pf_match.group(2).strip('[]"` '), "dbfileinput_query_output_field", f"SQL Query: {sql_query}", is_output=True)
                                self._add_field(pf_match.group(1).strip('[]"` '), "dbfileinput_query_source_field", f"SQL Query: {sql_query}", is_output=False)
                            else:
                                self._add_field(pf_with_alias.strip().strip('[]"` '), "dbfileinput_query_output_field", f"SQL Query: {sql_query}", is_output=True)
                    potential_sql_fields = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', sql_query)
                    for psf in potential_sql_fields:
                        if psf.upper() not in ['SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'ON', 'AS', 'GROUP', 'BY', 'ORDER', 'AND', 'OR', 'NOT']:
                               self._add_field(psf, "dbfileinput_query_referenced_field", f"SQL Query: {sql_query}", is_output=False)
                fields_list_node = configuration_node.find('SelectedFields')
                if fields_list_node is not None:
                    for field_tag in fields_list_node.findall('Field'):
                        field_name = field_tag.get('name')
                        self._add_field(field_name, "dbfileinput_table_output_field", "Selected from table/view", is_output=True)

            # --- CalgaryInput / CalgaryJoin ---
            elif self.plugin in ('CalgaryPluginsGui.CalgaryInput.CalgaryInput', 'CalgaryPluginsGui.CalgaryJoin.CalgaryJoin'):
                root_file_node = configuration_node.find('RootFileName')
                if root_file_node is not None and root_file_node.text:
                    self.calgary_root_filename = root_file_node.text.strip()
                query_node = configuration_node.find('Query')
                if query_node is not None and query_node.text and query_node.text.strip():
                    try:
                        inner_xml_root = ET.fromstring(query_node.text.strip())
                        for field_element in inner_xml_root.findall('.//Field'):
                            field_name = field_element.get('name')
                            self._add_field(field_name, "calgary_query_field", query_node.text.strip(), is_output=False)
                            self._add_field(field_name, "calgary_query_output_field", query_node.text.strip(), is_output=True)
                    except ET.ParseError: pass
                if self.plugin == 'CalgaryPluginsGui.CalgaryJoin.CalgaryJoin':
                    join_fields_container = configuration_node.find('JoinFields')
                    if join_fields_container is not None:
                        for jf_node in join_fields_container.findall('Field'):
                            index_field = jf_node.get('indexField')
                            stream_field = jf_node.get('streamField')
                            if index_field: self._add_field(index_field, "calgary_join_index_field", f"Index field, joins with stream field: {stream_field}", is_output=False)
                            if stream_field:
                                self._add_field(stream_field, "calgary_join_stream_field", f"Stream field, joins with index field: {index_field}", is_output=False)
                                self._add_field(stream_field, "calgary_join_output_field", f"From stream field in join", is_output=True)

            # --- CalgaryLoader Tool (Output) ---
            elif self.plugin == 'CalgaryLoadersGui.CalgaryLoader.CalgaryLoader':
                fields_config_node = configuration_node.find('Fields')
                if fields_config_node is not None:
                    for field_element in fields_config_node.findall('Field'):
                        field_name = field_element.get('field')
                        self._add_field(field_name, "calgaryloader_output_field", "Field loaded into Calgary", is_output=True)

            # --- TableauOutput Tool ---
            elif self.plugin and self.plugin.startswith('TableauOutput'):
                input_col_node = configuration_node.find('InputColumn1')
                if input_col_node is not None and input_col_node.text:
                    fields_str = input_col_node.text
                    for f_name in fields_str.split(','):
                        if f_name.strip(): self._add_field(f_name.strip(), "tableau_output_field", "Field sent to Tableau Output", is_output=True)

            # --- Generic DbFileOutput ---
            elif self.plugin == 'AlteryxBasePluginsGui.DbFileOutput.DbFileOutput':
                file_node = configuration_node.find('File')
                file_info = file_node.text if file_node is not None and file_node.text else "N/A"
                self._add_field("*AllIncomingFields*", "dbfileoutput_generic_output", f"Outputting all fields to: {file_info}", is_output=True)

            # --- InputData Tool ---
            elif self.plugin == 'AlteryxBasePluginsGui.InputData.InputData':
                fs_options = configuration_node.find('FormatSpecificOptions')
                if fs_options is not None:
                    field_names_node = fs_options.find('FieldNames')
                    if field_names_node is not None:
                        for field_tag in field_names_node.findall('Field'):
                            field_name = field_tag.get('name')
                            self._add_field(field_name, "inputdata_source_field", "Field from InputData tool (e.g. CSV/Excel)", is_output=True)

            # --- DynamicInput Tool ---
            elif self.plugin == 'AlteryxConnectorGui.DynamicInput.DynamicInput':
                input_source_template_node = configuration_node.find('InputSourceTemplate')
                if input_source_template_node is not None:
                    self._add_field("*FieldsFromDynamicInputTemplate*", "dynamicinput_template_field", "Fields defined by DynamicInput template", is_output=True)
        except Exception:
            pass

# --- SoT and Workflow Processing ---
def get_sot_downstream_tool_ids(root_xml_element, all_nodes_map, sot_filename_key):
    sot_initial_tool_ids = set()
    if not sot_filename_key: return set() # Simplified return
    for tool_id, node_obj in all_nodes_map.items():
        if node_obj.plugin in ('CalgaryPluginsGui.CalgaryInput.CalgaryInput', 'CalgaryPluginsGui.CalgaryJoin.CalgaryJoin'):
            if node_obj.calgary_root_filename and sot_filename_key in node_obj.calgary_root_filename:
                sot_initial_tool_ids.add(node_obj.tool_id)
    adj_list = defaultdict(list)
    connections_xml = root_xml_element.find('Connections')
    if connections_xml is not None:
        for conn_xml in connections_xml.findall('Connection'):
            origin_node = conn_xml.find('Origin')
            dest_node = conn_xml.find('Destination')
            if origin_node is not None and 'ToolID' in origin_node.attrib and dest_node is not None and 'ToolID' in dest_node.attrib:
                adj_list[origin_node.attrib['ToolID']].append(dest_node.attrib['ToolID'])
    downstream_from_sot_ids = set()
    queue = deque(sot_initial_tool_ids)
    visited_for_bfs = set(sot_initial_tool_ids)
    while queue:
        current_tool_id = queue.popleft()
        downstream_from_sot_ids.add(current_tool_id)
        for neighbor_tool_id in adj_list.get(current_tool_id, []):
            if neighbor_tool_id not in visited_for_bfs:
                visited_for_bfs.add(neighbor_tool_id)
                queue.append(neighbor_tool_id)
    return downstream_from_sot_ids

def process_single_workflow(filepath, sot_filename_key_optional):
    original_filename = os.path.basename(filepath)
    file_ext = filepath.split('.')[-1].lower()
    workflow_field_usages = []
    last_modified_date_str = "N/A"

    try:
        timestamp = os.path.getmtime(filepath) # Get mtime from original filepath
        last_modified_date_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except FileNotFoundError:
        print(f"Warning: Original file for mtime not found: {filepath}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Could not get mtime for {filepath}: {e}", file=sys.stderr)


    if not (file_ext == 'xml' or file_ext == 'yxmd'): return []

    xml_filepath_to_parse = filepath
    temp_xml_created = False
    if file_ext == 'yxmd':
        base_name = os.path.splitext(original_filename)[0]
        temp_xml_filepath = os.path.join(os.path.dirname(filepath), f"_temp_merged_{base_name}.xml")
        try:
            copyfile(filepath, temp_xml_filepath)
            xml_filepath_to_parse = temp_xml_filepath
            temp_xml_created = True
        except Exception as e:
            print(f"Error copying {original_filename} to temporary XML: {e}", file=sys.stderr)
            return []

    all_nodes_map = {}
    try:
        tree = ET.parse(xml_filepath_to_parse)
        root = tree.getroot()
        for node_xml_element in root.findall('.//Node'):
            try:
                node_obj = EnhancedNodeElement(node_xml_element)
                all_nodes_map[node_obj.tool_id] = node_obj
            except Exception: continue
        downstream_sot_tool_ids = set()
        if sot_filename_key_optional:
            downstream_sot_tool_ids = get_sot_downstream_tool_ids(root, all_nodes_map, sot_filename_key_optional)
        for tool_id, node_obj in all_nodes_map.items():
            is_downstream = 1 if sot_filename_key_optional and tool_id in downstream_sot_tool_ids else 0
            for field_entry in node_obj.extracted_fields:
                plugin_name = node_obj.plugin
                usage_criticality = TOOL_CRITICALITY_MAPPING.get(plugin_name, 0)
                if plugin_name and plugin_name.startswith('TableauOutput') and plugin_name not in TOOL_CRITICALITY_MAPPING:
                    usage_criticality = 4
                workflow_field_usages.append({
                    'FileName': original_filename,
                    'LastModified': last_modified_date_str, # ADDED
                    'ToolID': tool_id,
                    'Tool': plugin_name,
                    'FieldName': field_entry['field_name'],
                    'UsageContext': field_entry['usage_context'],
                    'FieldUsage': field_entry['detail'],
                    'IsDownstreamSOT': is_downstream,
                    'UsageCriticallity': usage_criticality
                })
    except ET.ParseError as e_parse: print(f"XML ParseError in {original_filename}: {e_parse}", file=sys.stderr)
    except Exception as e_proc: print(f"Unexpected error processing {original_filename}: {e_proc}", file=sys.stderr)
    finally:
        if temp_xml_created:
            try: os.remove(xml_filepath_to_parse)
            except OSError as e_os: print(f"Warning: Could not remove temp file {xml_filepath_to_parse}: {e_os}", file=sys.stderr)
    return workflow_field_usages

# --- Output Generation ---
def generate_output_b(all_field_usages_across_workflows, target_fields_for_output_b_set, sot_active):
    output_b_data = []
    if not target_fields_for_output_b_set: return []
    for usage_record in all_field_usages_across_workflows:
        if usage_record['FieldName'] in target_fields_for_output_b_set:
            if sot_active:
                if usage_record['IsDownstreamSOT'] == 1: output_b_data.append(usage_record)
            else: output_b_data.append(usage_record)
    return output_b_data

def load_fields_from_csv(csv_filepath):
    fields = set()
    if not csv_filepath or not os.path.exists(csv_filepath): return fields
    try:
        with open(csv_filepath, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row_num, row in enumerate(reader, 1):
                if row and row[0].strip(): fields.add(row[0].strip())
    except Exception as e: print(f"Error reading '{csv_filepath}': {e}", file=sys.stderr)
    if not fields: print(f"Warning: No field names loaded from '{csv_filepath}'.", file=sys.stderr)
    return fields

# --- Main Orchestration ---
def analyze_alteryx_ecosystem_merged(
    input_directory,
    output_b_csv_filename="output_B_detailed_usage.csv",
    sot_filename_key=None,
    output_b_target_fields_csv=None
    ):
    print(f"Starting Alteryx ecosystem analysis in directory: '{input_directory}'")
    sot_is_active = bool(sot_filename_key)
    if sot_is_active: print(f"Source of Truth (SoT) key: '{sot_filename_key}' (Lineage tracing enabled)")
    else: print("No Source of Truth (SoT) key provided. Lineage tracing for SoT is disabled.")

    output_b_target_fields = set()
    generate_output_b_flag = False
    if output_b_target_fields_csv:
        print(f"Loading target field names for Output B from: '{output_b_target_fields_csv}'")
        output_b_target_fields = load_fields_from_csv(output_b_target_fields_csv)
        if output_b_target_fields:
            generate_output_b_flag = True
            print(f"Loaded {len(output_b_target_fields)} unique fields for Output B.")
        else: print(f"Warning: No target fields loaded for Output B from '{output_b_target_fields_csv}'. Output B will not be generated.")
    else: print("No target fields CSV provided for Output B. Output B will not be generated.")

    all_field_usages_data = []
    if not os.path.isdir(input_directory):
        print(f"Error: Input directory '{input_directory}' not found.", file=sys.stderr)
        return
    workflow_files = [os.path.join(input_directory, f) for f in os.listdir(input_directory)
                      if os.path.isfile(os.path.join(input_directory, f)) and (f.lower().endswith('.yxmd') or f.lower().endswith('.xml'))]
    if not workflow_files:
        print(f"No .yxmd or .xml files found in '{input_directory}'.")
        return
    
    total_files = len(workflow_files)
    print(f"Found {total_files} workflow files to process.")

    for i, filepath in enumerate(workflow_files, 1):
        # Processing indicator
        # Use sys.stdout.write and flush for better control with \r
        progress_message = f"Processing file {i}/{total_files}: {os.path.basename(filepath)}..."
        sys.stdout.write(progress_message + " " * (80 - len(progress_message)) + "\r") # Pad to overwrite
        sys.stdout.flush()
        usages_in_file = process_single_workflow(filepath, sot_filename_key)
        all_field_usages_data.extend(usages_in_file)

    sys.stdout.write(" " * 80 + "\r") # Clear the progress line
    sys.stdout.flush()
    
    if not all_field_usages_data:
        print("No field usages found in any workflow.")
        return
    print(f"\nTotal field usage instances extracted: {len(all_field_usages_data)}")

    if generate_output_b_flag:
        print(f"\nGenerating Output B: Detailed Usage for {len(output_b_target_fields)} target field(s)...")
        data_for_output_b = generate_output_b(all_field_usages_data, output_b_target_fields, sot_is_active)
        if data_for_output_b:
            headers_b = ['FileName', 'LastModified', 'ToolID', 'Tool', 'FieldName',
                         'UsageContext', 'FieldUsage', 'IsDownstreamSOT', 'UsageCriticallity']
            try:
                with open(output_b_csv_filename, 'w', newline='', encoding='utf-8') as f_out_b:
                    writer_b = csv.DictWriter(f_out_b, fieldnames=headers_b)
                    writer_b.writeheader()
                    writer_b.writerows(data_for_output_b)
                print(f"Output B successfully written to '{output_b_csv_filename}'")
            except IOError as e: print(f"Error writing Output B to CSV '{output_b_csv_filename}': {e}", file=sys.stderr)
        else: print(f"No detailed usage found for the specified target fields for Output B {'(considering SoT if active)' if sot_is_active else ''}.")
    else: print("\nOutput B generation skipped as no target fields were specified or loaded.")
    print("\nAnalysis complete.")




################################################################################################
EXAMPLE USAGE
################################################################################################

WORKFLOWS_DIRECTORY_IN = "WHERE WORKFLOWS ARE BEING READ FROM"
TARGET_FIELD_NAMES = "FOR SOURCE OF TRUTH VALUE"
SOT_KEY = "KEY NAME FOR SOURCE OF TRUTH"
OUTPUT_FILENAME = "FILE NAME OUTPUT"

if __name__ == '__main__':

    print("\n===== SCENARIO 1: Full Analysis with SoT, Output B with target fields =====")
    analyze_alteryx_ecosystem_merged(
        input_directory=WORKFLOWS_DIRECTORY_IN,
        sot_filename_key=SOT_KEY,
        output_b_target_fields_csv=TARGET_FIELD_NAMES,
        output_b_csv_filename=f"{OUTPUT_FILENAME}.csv"
    )

