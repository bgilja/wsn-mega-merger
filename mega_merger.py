from pymote.algorithm import NodeAlgorithm
from pymote.message import Message

INF = 9999


class MegaMerger(NodeAlgorithm):
    required_params = ()
    default_params = {'neighborsKey': 'Neighbors'}
    
    def _calculate_road_distance(self, node1, node2):
        x1, y1 = node1.memory['position'][0], node1.memory['position'][1]
        x2, y2 = node2.memory['position'][0], node2.memory['position'][1]
        return abs(x1 - x2) + abs(y1 - y2)
    
    def _get_destination_from_id(self, node, destination_id):
        for neighbour in node.memory[self.neighborsKey]:
            if neighbour.id == destination_id:
                return neighbour
        return None
    
    def _handle_test_response(self, node, message):
        if node.memory['test_response_count'] < node.memory['test_request_count']:
            return
        
        if node.memory['find_response_count'] < node.memory['find_request_count']:
            return
        
        node.send(Message(
            destination=[node],
            header="Found"
        ))
    
    def _receive_message(self, node, message):
        print(node.id, node.memory['city_name'], node.memory['downtown_id'], node.memory['level'], message.header, node.status)
        
        actions = [
            ("Find", self.find),
            ("Test", self.test),
            ("Accept", self.accept),
            ("Reject", self.reject),
            ("Found", self.found),
            ("Connect", self.connect),
            ("Lets merge", self.lets_merge),
            ("Change root", self.change_root),
            ("Change root complete", self.change_root_complete),
        ]
        
        for header, action in actions:
            if header != message.header:
                continue
                
            action(node, message)
            return
            
        print("Extra message " + message.header)
    
    def initializer(self):
        self.cities = set()
        
        for node in self.network.nodes():
            self.cities.add(node.id)
            
            node.memory[self.neighborsKey] = node.compositeSensor.read()['Neighbors']
            
            node.memory['downtown_id'] = node.id
            node.memory['city_name'] = str(node.memory['downtown_id'])
            node.memory['level'] = 1
            node.memory['neighbours_in_city'] = []
            node.memory['sent_connection_request'] = None
            node.memory['received_connection_request'] = []
            node.memory['parent_district'] = None
            node.memory['position'] = node.network.pos[node]
            
            node.status = 'INITIATOR'
            self.network.outbox.insert(0, Message(
                header=NodeAlgorithm.INI,
                destination=node
            ))
        
    def initiator(self, node, message):
        node.send(Message(
            destination=[node],
            header='Find'
        ))
        node.status = 'AVAILABLE'
        
    def find(self, node, message):
        if message.header != "Find":
            return
        
        node.memory['minimal_road_distance'] = INF
        node.memory['minimal_road_distance_node_id'] = None
        
        node.memory['test_response_count'] = 0
        node.memory['test_request_count'] = 0
        
        node.memory['find_response_count'] = 0
        node.memory['find_request_count'] = 0
        
        node.memory['change_root_response_count'] = 0
        node.memory['change_root_request_count'] = 0
        
        node.memory['sent_connection_request'] = None
        
        for neighbour in node.memory[self.neighborsKey]:
            if neighbour in node.memory['neighbours_in_city']:
                continue

            node.memory['test_request_count'] += 1

            data = {
                'start_node_id': node.id,
                'destination_node_id': neighbour.id,
                'downtown_id': node.memory['downtown_id'],
                'distance': self._calculate_road_distance(node, neighbour)
            }

            node.send(Message(
                destination=[neighbour],
                header='Test',
                data=data
            ))

        for neighbour_in_city in node.memory['neighbours_in_city']:
            parent = node.memory['parent_district']
            if node.memory['parent_district'] is not None and neighbour_in_city.id == parent.id:
                continue

            node.memory['find_request_count'] += 1
            
            node.send(Message(
                destination=[neighbour_in_city],
                header='Find',
                data={}
            ))

        node.status = 'FINDING'
            

    def test(self, node, message):
        if message.header != "Test":
            return
        
        destination = self._get_destination_from_id(node, message.data['start_node_id'])
        if destination is None:
            return
        
        if message.data['downtown_id'] == node.memory['downtown_id']:
            node.send(Message(
                destination=[destination],
                header='Reject',
                data=message.data
            ))
        else:
            node.send(Message(
                destination=[destination],
                header='Accept',
                data=message.data
            ))

    def accept(self, node, message):
        if message.header != "Accept":
            return
        
        node.memory['test_response_count'] += 1
        if node.memory['minimal_road_distance'] > message.data['distance']:
            node.memory['minimal_road_distance'] = message.data['distance']
            node.memory['minimal_road_distance_node_id'] = message.data['destination_node_id']
            
        self._handle_test_response(node, message)

    def reject(self, node, message):
        if message.header != "Reject":
            return
        
        node.memory['test_response_count'] += 1   
        self._handle_test_response(node, message)

    def found(self, node, message):
        if message.header != "Found":
            return
        
        node.memory['find_response_count'] += 1
        
        if node.memory['test_response_count'] < node.memory['test_request_count']:
            return
        
        if node.memory['find_response_count'] < node.memory['find_request_count']:
            return
        
        data = {
            'start_node_id': node.id,
            'destination_node_id': node.memory['minimal_road_distance_node_id'],
            'minimal_road_distance': node.memory['minimal_road_distance'],
            'level': node.memory['level']
        }
            
        if node.id == node.memory['downtown_id']:
            node.send(Message(
                destination=[node],
                header="Connect",
                data=data
            ))
        else:
            node.send(Message(
                destination=[node.memory['parent_district']],
                header='Found',
                data=data
            ))
            
        node.status = 'AVAILABLE'

    def connect(self, node, message):
        if message.header != "Connect":
            return
        
        if node.id == message.data["start_node_id"]:
            for neighbour in node.memory[self.neighborsKey]:
                if neighbour in node.memory['neighbours_in_city']:
                    continue
                
                if neighbour.id != message.data["destination_node_id"]:
                    continue
                
                node.memory['sent_connection_request'] = message.data
                
                node.send(Message(
                    destination=[neighbour],
                    header='Lets merge',
                    data=message.data
                ))
                
        for neighbour_in_city in node.memory['neighbours_in_city']:
            parent = node.memory['parent_district']
            if node.memory['parent_district'] is not None and neighbour_in_city.id == parent.id:
                continue

            node.send(Message(
                destination=[neighbour_in_city],
                header='Connect',
                data=message.data
            ))

        node.status = 'CONNECTING'

    def lets_merge(self, node, message):
        if message.header != "Lets merge":
            return
        
        node.memory['received_connection_request'].append(message.data)
        
        sent_connection_request = node.memory['sent_connection_request']
        if sent_connection_request is None:
            return
        
        if sent_connection_request["destination_node_id"] != message.data["start_node_id"] or \
            sent_connection_request["start_node_id"] != message.data["destination_node_id"]:
            return
        
        if sent_connection_request["level"] == node.memory['level']:
            # min is new downtown, max is new district
            min_node_id = min(message.data["start_node_id"], message.data["destination_node_id"])
            max_node_id = max(message.data["start_node_id"], message.data["destination_node_id"])
            
            new_city_name = str(min_node_id) + "_" + str(max_node_id)
            new_level = node.memory['level'] + 1
            
            if node.id == min_node_id:
                other_node = self._get_destination_from_id(node, max_node_id)
                                
                node.send(Message(
                    destination=[other_node],
                    header='Change root',
                    data={
                        'new_downtown_id': min_node_id,
                        'new_city_name': new_city_name,
                        'new_level': new_level,
                        'new_parent_district': node,
                    }
                ))
            else:
                other_node = self._get_destination_from_id(node, min_node_id)
                                
                node.send(Message(
                    destination=[other_node],
                    header='Change root',
                    data={
                        'new_downtown_id': min_node_id,
                        'new_city_name': new_city_name,
                        'new_level': new_level,
                        'new_parent_district': node,
                    }
                ))
        elif sent_connection_request["level"] < node.memory['level']:
            print("ABSORPTION", "LETS MERGE", node.id, message.data)
            node.status = 'CHANGING_ROOT'
        else:
            print("SUSPEND", "LETS MERGE", node.id, message.data)

    def change_root(self, node, message):
        if message.header != "Change root":
            return
        
        if message.data["new_city_name"] == node.memory["city_name"]:
            return
        
        if message.data["new_parent_district"].id not in [n.id for n in node.memory['neighbours_in_city']]:
            node.memory['neighbours_in_city'].append(message.data["new_parent_district"])
        
        node.memory["downtown_id"] = message.data["new_downtown_id"]
        node.memory["city_name"] = message.data["new_city_name"]
        node.memory["level"] = message.data["new_level"]
        
        if node.id == node.memory["downtown_id"]:
            node.memory["parent_district"] = None
        else:
            node.memory["parent_district"] = message.data["new_parent_district"]
        
        for neighbour_in_city in node.memory['neighbours_in_city']:
            new_parent = message.data["new_parent_district"]
            if message.data["new_parent_district"] is not None and \
                neighbour_in_city.id == new_parent.id:
                continue
                
            node.memory['change_root_request_count'] += 1
            
            node.send(Message(
                destination=[neighbour_in_city],
                header='Change root',
                data={
                    'new_downtown_id': message.data["new_downtown_id"],
                    'new_city_name': message.data["new_city_name"],
                    'new_level': message.data["new_level"],
                    'parent_district': node,
                }
            ))
        
        if node.memory['change_root_request_count'] == 0:
            node.send(Message(
                destination=[node],
                header="Change root complete"
            ))
        
        node.status = 'CHANGING_ROOT'

    def change_root_complete(self, node, message):
        if message.header != "Change root complete":
            return
        
        if node.id == node.memory['downtown_id']:
            return
        
            node.send(Message(
                destination=[node],
                header='Find',
            ))
        else:
            node.memory['change_root_response_count'] += 1

            if node.memory['change_root_response_count'] < node.memory['change_root_request_count'] + 1:
                return

            node.send(Message(
                destination=[node.memory['parent_district']],
                header='Change root complete',
            ))
        
        node.status = 'AVAILABLE'
        

    def done(self, node, message):
        pass
    
    
    STATUS = {
        'AVAILABLE': _receive_message,
        'FINDING': _receive_message,
        'CONNECTING': _receive_message,
        'CHANGING_ROOT': _receive_message,
        'INITIATOR': initiator,
        'DONE': done,
    }
