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
        
        self.found(node, Message(header="Found"))
    
    def _receive_message(self, node, message):
        # print(node.id, node.memory['city_name'], node.memory['downtown_id'], node.memory['level'], message.header)
        
        actions = [
            ("Find", self.find),
            ("Test", self.test),
            ("Accept", self.accept),
            ("Reject", self.reject),
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
            node.memory['level'] = 0
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
        self.find(node, Message(
            header="Find",
            destination=[node]
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

        node.status = 'FIND'
            

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
        node.memory['find_response_count'] += 1
        
        if node.memory['test_response_count'] < node.memory['test_request_count']:
            return
        
        if node.memory['find_response_count'] < node.memory['find_request_count']:
            return
        
        data = {
            'start_node_id': node.id,
            'destination_node_id': node.memory['minimal_road_distance_node_id'],
            'minimal_road_distance': node.memory['minimal_road_distance']
        }
            
        if node.memory['parent_district'] is None:
            self.connect(node, Message(destination=[node], header="Connect", data=data))
        else:
            node.send(Message(
                destination=[node.memory['parent_district']],
                header='Found',
                data=data
            ))
            
        node.status = 'AVAILABLE'

    def connect(self, node, message):
        print("Connect", node.id, message.data)
        pass

    def lets_merge(self, node, message):
        pass

    def change_root(self, node, message):
        pass

    def change_root_complete(self, node, message):
        pass

    def done(self, node, message):
        pass
    
    
    STATUS = {
        'AVAILABLE': _receive_message,
        'FIND': _receive_message,
        'INITIATOR': initiator,
        'DONE': done,
    }
