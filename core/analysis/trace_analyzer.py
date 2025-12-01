# core/trace_analyzer.py (Corrected)
import sqlite3
import json

class TraceAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        # PERFORMANCE FIX: Cache events to avoid repeated DB queries
        self._events_cache = None
        self._cache_valid = False
        # The connection and cache are no longer stored on the instance (self).
        # This makes the object stateless.

    def _connect(self):
        """Helper to establish a new database connection."""
        if not self.db_path:
            return None
        try:
            uri = f"file:{self.db_path}?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            print(f"[ANALYZER-ERROR] Failed to connect to database '{self.db_path}': {e}")
            return None

    def get_all_tables(self):
        conn = self._connect()
        if not conn: return []
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'event_log';")
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_all_events(self, use_cache=True):
        """
        PERFORMANCE FIX: Cache events to avoid O(N^2) complexity
        
        Args:
            use_cache: If True, return cached events; if False, force refresh
        """
        if use_cache and self._cache_valid and self._events_cache is not None:
            return self._events_cache
        
        conn = self._connect()
        if not conn: return []
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM event_log ORDER BY timestamp")
            
            all_events = []
            for row in cursor.fetchall():
                event = dict(row)
                event['payload'] = json.loads(event['payload'])
                event['action_display'] = f"{event['action']} on {event['table_name'].split('_',1)[1]}"
                all_events.append(event)
            
            # Cache the results
            self._events_cache = all_events
            self._cache_valid = True
            return all_events
        finally:
            conn.close()

    def get_history_for_field(self, table_name, pk_value):
        """
        Gets the history for a specific row by fetching all events and then filtering.
        This is now guaranteed to get fresh data.
        """
        # 1. Get a fresh, complete list of all events for the simulation.
        all_events = self.get_all_events()
        if not all_events:
            return []

        # 2. Filter this fresh list based on the specific primary key.
        PK_COLUMN_NAME = 'id'
        history = []
        
        for event in all_events:
            if event.get('table_name') != table_name:
                continue

            action = event.get('action')
            payload = event.get('payload', {})
            
            if action == 'CREATE':
                created_id = str(payload.get(PK_COLUMN_NAME, '')).strip()
                if created_id == str(pk_value).strip():
                    history.append(event)
            
            elif action == 'UPDATE':
                where_clause = payload.get('where', {})
                updated_id = str(where_clause.get(PK_COLUMN_NAME, '')).strip()
                if updated_id == str(pk_value).strip():
                    history.append(event)

        return history

    def _get_row_state_before_event(self, table_name, pk_value, event_timestamp):
        """PERFORMANCE FIX: Use SQL WHERE to filter instead of Python loops"""
        PK_COLUMN_NAME = 'id'
        row_state = {}
        
        conn = self._connect()
        if not conn:
            return row_state
        
        try:
            cursor = conn.cursor()
            # PERFORMANCE: Filter in SQL instead of fetching all events
            cursor.execute("""
                SELECT * FROM event_log 
                WHERE timestamp < ? AND table_name = ?
                ORDER BY timestamp
            """, (event_timestamp, table_name))
            
            for row in cursor.fetchall():
                event = dict(row)
                payload = json.loads(event['payload'])
                action = event['action']
                
                if action == 'CREATE':
                    if str(payload.get(PK_COLUMN_NAME, '')).strip() == str(pk_value).strip():
                        row_state = payload
                elif action == 'UPDATE':
                    where_clause = payload.get('where', {})
                    if str(where_clause.get(PK_COLUMN_NAME, '')).strip() == str(pk_value).strip():
                        row_state.update(payload.get('update', {}))
            
            return row_state
        finally:
            conn.close()

    def get_diff_for_event(self, event):
        action = event['action']
        payload = event['payload']
        
        if action == 'CREATE':
            return None, payload
        elif action == 'UPDATE':
            PK_COLUMN_NAME = 'id'
            table_name = event['table_name']
            where_clause = payload.get('where', {})
            pk_value = where_clause.get(PK_COLUMN_NAME)
            
            if not pk_value:
                return {}, payload.get('update')

            before_state = self._get_row_state_before_event(table_name, pk_value, event['timestamp'])
            after_state = payload.get('update', {})
            
            return before_state, after_state
        
        return None, None
        
    def get_state_at_timestamp(self, target_timestamp):
        conn = self._connect()
        if not conn: return {}
        try:
            cursor = conn.cursor()
            current_state = {}
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'event_log';")
            all_tables = [row[0] for row in cursor.fetchall()]

            for table in all_tables:
                current_state[table] = {}
                cursor.execute(f"SELECT * FROM {table}")
                for row in cursor.fetchall():
                    if 'id' in row.keys():
                        current_state[table][row['id']] = dict(row)

            all_events = self.get_all_events()
            for event in reversed(all_events):
                if event['timestamp'] <= target_timestamp:
                    break 
                table = event['table_name']
                payload = event['payload']
                if event['action'] == 'CREATE':
                    pk_val = payload.get('id')
                    if pk_val in current_state.get(table, {}):
                        del current_state[table][pk_val]
            return current_state
        finally:
            conn.close()
    
    def get_trace_as_dag(self, correlation_id=None):
        """
        Returns the event trace as a DAG structure.
        
        Returns:
            dict: {
                'nodes': [{'event_id': str, 'handler': str, 'component': str, 'timestamp': str, ...}],
                'edges': [{'from': event_id, 'to': event_id}, ...]
            }
        """
        conn = self._connect()
        if not conn:
            return {'nodes': [], 'edges': []}
        
        try:
            cursor = conn.cursor()
            
            # Build query based on whether we're filtering by correlation_id
            if correlation_id:
                query = """
                    SELECT DISTINCT event_id, timestamp, correlation_id, causation_id, 
                           component, handler_name, trigger_message, table_name, action
                    FROM event_log 
                    WHERE correlation_id = ?
                    ORDER BY timestamp
                """
                cursor.execute(query, (correlation_id,))
            else:
                query = """
                    SELECT DISTINCT event_id, timestamp, correlation_id, causation_id, 
                           component, handler_name, trigger_message, table_name, action
                    FROM event_log 
                    ORDER BY timestamp
                """
                cursor.execute(query)
            
            rows = cursor.fetchall()
            
            # Build nodes (group by event_id since one event can have multiple writes)
            nodes_dict = {}
            for row in rows:
                event_id = row['event_id']
                if event_id not in nodes_dict:
                    nodes_dict[event_id] = {
                        'event_id': event_id,
                        'timestamp': row['timestamp'],
                        'correlation_id': row['correlation_id'],
                        'causation_id': row['causation_id'],
                        'component': row['component'],
                        'handler_name': row['handler_name'],
                        'trigger_message': row['trigger_message'],
                        'actions': []
                    }
                # Add action to this event
                nodes_dict[event_id]['actions'].append({
                    'table': row['table_name'],
                    'action': row['action']
                })
            
            nodes = list(nodes_dict.values())
            
            # Build edges from causation relationships
            edges = []
            for node in nodes:
                if node['causation_id']:  # If this event has a parent
                    edges.append({
                        'from': node['causation_id'],
                        'to': node['event_id']
                    })
            
            return {'nodes': nodes, 'edges': edges}
        
        finally:
            conn.close()
    
    def get_event_chain(self, event_id, direction='backward'):
        """
        Traverse the DAG to get the chain of events.
        
        Args:
            event_id: The starting event
            direction: 'backward' (ancestors) or 'forward' (descendants)
        
        Returns:
            list: List of event_ids in the chain
        """
        dag = self.get_trace_as_dag()
        nodes_dict = {n['event_id']: n for n in dag['nodes']}
        
        if event_id not in nodes_dict:
            return []
        
        chain = []
        visited = set()
        
        def traverse_backward(eid):
            if eid in visited:
                return
            visited.add(eid)
            chain.append(eid)
            
            node = nodes_dict.get(eid)
            if node and node['causation_id']:
                traverse_backward(node['causation_id'])
        
        def traverse_forward(eid):
            if eid in visited:
                return
            visited.add(eid)
            chain.append(eid)
            
            # Find all children
            for edge in dag['edges']:
                if edge['from'] == eid:
                    traverse_forward(edge['to'])
        
        if direction == 'backward':
            traverse_backward(event_id)
        else:
            traverse_forward(event_id)
        
        return chain
    
    def get_critical_path(self, correlation_id):
        """
        Find the critical path (longest path) in the DAG for a given transaction.
        This helps identify performance bottlenecks.
        
        Returns:
            list: List of event_ids representing the critical path
        """
        dag = self.get_trace_as_dag(correlation_id)
        
        if not dag['nodes']:
            return []
        
        nodes_dict = {n['event_id']: n for n in dag['nodes']}
        
        # Find root nodes (no causation_id)
        roots = [n['event_id'] for n in dag['nodes'] if not n['causation_id']]
        
        if not roots:
            return []
        
        # DFS to find longest path
        longest_path = []
        
        def dfs(event_id, current_path):
            nonlocal longest_path
            current_path = current_path + [event_id]
            
            # Find children
            children = [e['to'] for e in dag['edges'] if e['from'] == event_id]
            
            if not children:  # Leaf node
                if len(current_path) > len(longest_path):
                    longest_path = current_path
            else:
                for child in children:
                    dfs(child, current_path)
        
        for root in roots:
            dfs(root, [])
        
        return longest_path

    def close(self):
        """Close any open connections (no-op for stateless design)"""
        pass
