from pymote.algorithm import NodeAlgorithm
from pymote.message import Message

INF = 9999


# Abstract
class BaseMegaMerger(NodeAlgorithm):
    processed_messages = set()
    messages_sent = 0
    
    def add_road(self, node1, node2):
        node1_neighbour_ids = [n.id for n in node1.memory['neighbours_in_city']]
        if node2.id not in node1_neighbour_ids:
            node1.memory['neighbours_in_city'].append(node2)
            
        node2_neighbour_ids = [n.id for n in node2.memory['neighbours_in_city']]
        if node1.id not in node2_neighbour_ids:
            node2.memory['neighbours_in_city'].append(node1)

    def calculate_road_distance(self, node1, node2):
        x1, y1 = node1.memory['position'][0], node1.memory['position'][1]
        x2, y2 = node2.memory['position'][0], node2.memory['position'][1]
        return abs(x1 - x2) + abs(y1 - y2)
    
    def change_status(self, node, status):
        print("Node {} change status from {} to: {}".format(node.id, node.status, status))
        node.status = status
    
    def determine_new_city_name(self, node1, node2):
        if node1.memory["level"] == 1:
            node1_districts = [str(node1.id)]
        else:
            node1_districts = node1.memory['city_name'].replace("(", "").replace(")", "").split('_')
        
        if node2.memory["level"] == 1:
            node2_districts = [str(node2.id)]
        else:
            node2_districts = node2.memory['city_name'].replace("(", "").replace(")", "").split('_')
            
        districts = list(set(node1_districts + node2_districts))
        return "({})".format("_".join(districts))
    
    # Returns neighbour with selected id
    def get_neighbour_by_id(self, node, destination_id):
        for neighbour in node.memory[self.neighborsKey]:
            if neighbour.id == destination_id:
                return neighbour
        return None
    
    def get_node_by_id(self, node_id):
        for node in self.network.nodes():
            if node.id == node_id:
                return node
        return None
    
    def handle_test_response(self, node, message):
        if node.memory['test_response_count'] < node.memory['test_request_count']:
            return
        
        self.found(node, Message(
            destination=[node],
            header="Found",
            data={
                'minimal_road_distance': INF,
                'start_node_id': None,
                'destination_node_id': None,
            }
        ))

    def send_message(self, node, message):
        message.data["source_node_id"] = node.id
        message.data["message_id"] = self.messages_sent + 1

        self.messages_sent += 1
        node.send(message)


class MegaMerger(BaseMegaMerger):
    required_params = ()
    default_params = {'neighborsKey': 'Neighbors'}
    
    def receive_message(self, node, message):
        if message.data["message_id"] in self.processed_messages:
            return
        
        self.processed_messages.add(message.data["message_id"])
        
        actions = [
            ("Find", self.find, "AVAILABLE", True),
            ("Test", self.test, None, True),
            ("Accept", self.accept, "FINDING", True),
            ("Reject", self.reject, "FINDING", True),
            ("Found", self.found, "FINDING", True),
            ("Connect", self.connect, "AVAILABLE", True),
            ("Lets merge", self.lets_merge, None, True),
            ("Change root", self.change_root, None, True),
            ("Change root complete", self.change_root_complete, "CHANGING_ROOT", True),
        ]
        
        for header, action, required_status, log_message in actions:
            if header != message.header:
                continue
                
            if log_message:
                print(
                    message.data["message_id"],
                    node.id,
                    node.memory['city_name'],
                    node.memory['downtown_id'],
                    node.memory['level'],
                    message.header,
                    node.status,
                    message.data['source_node_id'],
                 )
            
            if required_status is not None and node.status != required_status:
                print("Node status ({}) is not required status {}".format(node.status, required_status))
                return
                
            action(node, message)
            return
            
        print("Extra message " + message.header)
    
    def initializer(self):
        for node in self.network.nodes():
            node.memory[self.neighborsKey] = node.compositeSensor.read()['Neighbors']
            
            node.memory['downtown_id'] = node.id
            node.memory['city_name'] = str(node.memory['downtown_id'])
            node.memory['level'] = 1
            node.memory['neighbours_in_city'] = []
            node.memory['received_connection_request'] = []
            node.memory['parent_district'] = None
            node.memory['position'] = node.network.pos[node]
            
            self.change_status(node, 'INITIATOR')
            self.network.outbox.insert(0, Message(
                header=NodeAlgorithm.INI,
                destination=node
            ))
        
    def initiator(self, node, message):
        self.change_status(node, 'AVAILABLE')
        self.find(node, Message(
            destination=[node],
            header="Find"
        ))
        
    def find(self, node, message):
        if message.header != "Find":
            return
        
        node.memory['minimal_road_distance'] = INF
        node.memory['minimal_road_distance_start_node_id'] = None
        node.memory['minimal_road_distance_destination_node_id'] = None
        
        node.memory['test_response_count'] = 0
        node.memory['test_request_count'] = 0
        
        node.memory['find_response_count'] = 0
        node.memory['find_request_count'] = 0
        
        node.memory['sent_connection_request'] = None
        
        neighbour_ids = [n.id for n in node.memory['neighbours_in_city']]
        for neighbour in node.memory[self.neighborsKey]:
            if neighbour.id in neighbour_ids:
                print("Neighbour {} is in same city, skipping".format(neighbour.id))
                continue

            node.memory['test_request_count'] += 1

            data = {
                'start_node_id': node.id,
                'destination_node_id': neighbour.id,
                'downtown_id': node.memory['downtown_id'],
                'distance': self.calculate_road_distance(node, neighbour)
            }

            self.send_message(node, Message(
                destination=[neighbour],
                header='Test',
                data=data
            ))

        for neighbour_in_city in node.memory['neighbours_in_city']:
            if node.memory['parent_district'] is not None and \
                neighbour_in_city.id == node.memory['parent_district'].id:
                print("Neighbour in city {} is parent, skipping".format(neighbour_in_city.id))
                continue

            node.memory['find_request_count'] += 1
            
            self.send_message(node, Message(
                destination=[neighbour_in_city],
                header='Find'
            ))
        
        self.change_status(node, 'FINDING')
        
        if node.memory['test_request_count'] == 0:
            self.handle_test_response(node, None)
            

    def test(self, node, message):
        if message.header != "Test":
            return
        
        destination = self.get_neighbour_by_id(node, message.data['start_node_id'])
        if message.data['downtown_id'] == node.memory['downtown_id']:
            self.send_message(node, Message(
                destination=[destination],
                header='Reject',
                data=message.data
            ))
        else:
            self.send_message(node, Message(
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
            node.memory['minimal_road_distance_start_node_id'] = message.data['start_node_id']
            node.memory['minimal_road_distance_destination_node_id'] = message.data['destination_node_id']
            
        self.handle_test_response(node, message)

    def reject(self, node, message):
        if message.header != "Reject":
            return
        
        node.memory['test_response_count'] += 1   
        self.handle_test_response(node, message)

    def found(self, node, message):
        if message.header != "Found":
            return
        
        node.memory['find_response_count'] += 1
        
        if node.memory['minimal_road_distance'] > message.data['minimal_road_distance']:
            node.memory['minimal_road_distance_start_node_id'] = message.data['start_node_id']
            node.memory['minimal_road_distance_destination_node_id'] = message.data['destination_node_id']
            node.memory['minimal_road_distance'] = message.data['minimal_road_distance']
        
        if node.memory['test_response_count'] < node.memory['test_request_count']:
            print("Found {} test_response_count ({}) is lower than test_request_count ({}), skipping".format(
                node.id, node.memory['test_response_count'], node.memory['test_request_count'])
            )
            return
        
        find_request_count = node.memory['find_request_count'] + 1
        if node.memory['find_response_count'] < find_request_count:
            print("Found {} find_response_count ({}) is lower than find_request_count ({}), skipping".format(
                node.id, node.memory['find_response_count'], find_request_count)
            )
            return
        
        data = {
            'start_node_id': node.memory['minimal_road_distance_start_node_id'],
            'destination_node_id': node.memory['minimal_road_distance_destination_node_id'],
            'minimal_road_distance': node.memory['minimal_road_distance'],
            'level': node.memory['level'],
        }
        
        self.change_status(node, 'AVAILABLE')
        if node.id == node.memory['downtown_id']:
            print("Node {} is downtown, connect".format(node.id))
            self.connect(node, Message(
                destination=[node],
                header="Connect",
                data=data
            ))
        else:
            print("Node {} is not downtown, send found message to {}".format(node.id, node.memory['parent_district'].id))
            self.send_message(node, Message(
                destination=[node.memory['parent_district']],
                header='Found',
                data=data
            ))

    def connect(self, node, message):
        if message.header != "Connect":
            return
        
        minimal_road_distance = message.data["minimal_road_distance"]
        if node.id == message.data["start_node_id"] and minimal_road_distance != INF:
            neighbours_in_city_ids = [n.id for n in node.memory['neighbours_in_city']]
            for neighbour in node.memory[self.neighborsKey]:
                if neighbour.id in neighbours_in_city_ids:
                    print("(Node {}): neighbour {} is in same city, skipping".format(node.id, neighbour.id))
                    continue

                if neighbour.id != message.data["destination_node_id"]:
                    print("(Node {}): neighbour {} is not destination {}, skipping".format(node.id, neighbour.id, message.data["destination_node_id"]))
                    continue
                
                received_request = None
                for received in node.memory['received_connection_request']:
                    if received["start_node_id"] != message.data["destination_node_id"]:
                        continue
                    received_request = received
                    
                if received_request is not None:
                    other_node = self.get_neighbour_by_id(node, received_request["start_node_id"])
                    self.add_road(node, other_node)
                    
                    downtown_node = self.get_node_by_id(node.memory["downtown_id"])

                    self.send_message(node, Message(
                        destination=[downtown_node],
                        header='Change root',
                        data={
                            'new_downtown_id': node.memory["downtown_id"],
                            'new_city_name': self.determine_new_city_name(other_node, node),
                            'new_level': node.memory["level"],
                            'new_parent_district': downtown_node,
                            'merge_type': "absorption"
                        }
                    ))
                else:
                    node.memory['sent_connection_request'] = message.data
                    
                    self.send_message(node, Message(
                        destination=[neighbour],
                        header='Lets merge',
                        data=message.data
                    ))
        else:
            print("Node {} connect: INFINITE DISTANCE".format(node.id))
                
        for neighbour_in_city in node.memory['neighbours_in_city']:
            parent = node.memory['parent_district']
            if node.memory['parent_district'] is not None and neighbour_in_city.id == parent.id:
                continue

            self.send_message(node, Message(
                destination=[neighbour_in_city],
                header='Connect',
                data=message.data
            ))
        
        if minimal_road_distance == INF:
            self.change_status(node, 'DONE')
            return
        
        self.change_status(node, 'CONNECTING')

    def lets_merge(self, node, message):
        if message.header != "Lets merge":
            return
        
        node.memory['received_connection_request'].append(message.data)
        
        if message.data["level"] < node.memory['level']:
            print("ABSORPTION", "LETS MERGE", node.id, node.memory['city_name'], message.data)
            return
        
        sent_connection_request = node.memory['sent_connection_request']
        
        if sent_connection_request is None:
            print("SUSPEND (no request)", "LETS MERGE", node.id, node.memory['city_name'], message.data)
            return
        
        if sent_connection_request["destination_node_id"] != message.data["start_node_id"] or \
            sent_connection_request["start_node_id"] != message.data["destination_node_id"]:
            print("SUSPEND (different road)", "LETS MERGE", node.id, node.memory['city_name'], message.data, sent_connection_request)
            return
        
        if message.data["level"] > node.memory['level']:
            print("SUSPEND", "LETS MERGE", node.id, node.memory['city_name'], message.data)
            return
        
        # proceed with friendly merger
        min_node_id = min(message.data["start_node_id"], message.data["destination_node_id"]) # new downtown id
        max_node_id = max(message.data["start_node_id"], message.data["destination_node_id"]) # other node id

        new_level = node.memory['level'] + 1

        if node.id == min_node_id:
            other_node = self.get_neighbour_by_id(node, max_node_id)
            self.add_road(node, other_node)
            
            self.change_root(node, Message(
                destination=[node],
                header='Change root',
                data={
                    'new_downtown_id': node.id,
                    'new_city_name': self.determine_new_city_name(node, other_node),
                    'new_level': new_level,
                    'new_parent_district': None,
                    'merge_type': "frieldy"
                }
            ))
        else:
            other_node = self.get_neighbour_by_id(node, min_node_id)
            self.add_road(node, other_node)
            
            self.send_message(node, Message(
                destination=[other_node],
                header='Change root',
                data={
                    'new_downtown_id': min_node_id,
                    'new_city_name': self.determine_new_city_name(other_node, node),
                    'new_level': new_level,
                    'new_parent_district': None,
                    'merge_type': "frieldy"
                }
            ))

    def change_root(self, node, message):
        if message.header != "Change root":
            return
        
        node.memory['change_root_response_count'] = 0
        node.memory['change_root_request_count'] = 0
        
        node.memory["downtown_id"] = message.data["new_downtown_id"]
        node.memory["city_name"] = message.data["new_city_name"]
        node.memory["level"] = message.data["new_level"]
        node.memory["parent_district"] = message.data["new_parent_district"]
        
        for neighbour_in_city in node.memory['neighbours_in_city']:
            if node.memory["parent_district"] is not None and \
                neighbour_in_city.id == node.memory["parent_district"].id:
                continue
                
            node.memory['change_root_request_count'] += 1
            
            self.send_message(node, Message(
                destination=[neighbour_in_city],
                header='Change root',
                data={
                    'new_downtown_id': message.data["new_downtown_id"],
                    'new_city_name': message.data["new_city_name"],
                    'new_level': message.data["new_level"],
                    'new_parent_district': node,
                    'merge_type': message.data["merge_type"]
                }
            ))
        
        self.change_status(node, 'CHANGING_ROOT')
        self.change_root_complete(node, Message(
            destination=[node],
            header="Change root complete"
        ))

    def change_root_complete(self, node, message):
        if message.header != "Change root complete":
            return
        
        node.memory['change_root_response_count'] += 1
        print(node.id, 'change_root_response_count', node.memory['change_root_response_count'], node.memory['change_root_request_count']+1)

        if node.memory['change_root_response_count'] < node.memory['change_root_request_count'] + 1:
            return
        
        node.status = "AVAILABLE"
        if node.id == node.memory['downtown_id']:
            self.find(node, Message(
                destination=[node],
                header='Find'
            ))
        else:
            self.send_message(node, Message(
                destination=[node.memory['parent_district']],
                header='Change root complete'
            ))
        
    def done(self, node, message):
        pass
    
    STATUS = {
        'AVAILABLE': receive_message,
        'FINDING': receive_message,
        'CONNECTING': receive_message,
        'CHANGING_ROOT': receive_message,
        'INITIATOR': initiator,
        'DONE': done,
    }
