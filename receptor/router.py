import datetime
import heapq
import logging
import random
from collections import defaultdict

from .exceptions import ReceptorBufferError, UnrouteableError
from .messages.framed import FramedMessage
from .stats import route_counter, route_info

logger = logging.getLogger(__name__)


class MeshRouter:
    _nodes = set()
    _edges = set()
    response_registry = dict()

    def __init__(self, receptor):
        self.receptor = receptor
        self.node_id = receptor.node_id
        route_info.info(dict(edges="()"))

    def node_is_known(self, node_id):
        return node_id in self._nodes or node_id == self.node_id

    def find_edge(self, left, right):
        node_actual = sorted([left, right])
        for edge in self._edges:
            if node_actual[0] == edge[0] and node_actual[1] == edge[1]:
                return edge
        return None

    def add_edges(self, edges):
        for edge in edges:
            existing_edge = self.find_edge(edge[0], edge[1])
            if existing_edge and existing_edge[2] > edge[2]:
                self.update_node(edge[0], edge[1], edge[2])
            else:
                self.register_edge(*edge)

    def register_edge(self, left, right, cost):
        if left != self.node_id:
            self._nodes.add(left)
        if right != self.node_id:
            self._nodes.add(right)
        edge = self.update_node(left, right, cost)
        if not edge:
            self._edges.add((*sorted([left, right]), cost))
        route_info.info(dict(edges=str(self._edges)))

    def update_node(self, left, right, cost):
        edge = self.find_edge(left, right)
        if edge:
            new_edge = (edge[0], edge[1], cost)
            self._edges.remove(edge)
            self._edges.add(new_edge)
            return edge
        return None

    def remove_node(self, node):
        edge = self.find_edge(self.node_id, node)
        if edge:
            self._edges.remove(edge)
            return edge
        return None

    def get_edges(self):
        """Returns set of edges"""
        return list(self._edges)

    def get_nodes(self):
        return self._nodes

    async def ping_node(self, node_id, expected_response=True):
        logger.info(f'Sending ping to node {node_id}')
        now = datetime.datetime.utcnow().isoformat()
        message = FramedMessage(header=dict(
            sender=self.node_id,
            recipient=node_id,
            timestamp=now,
            directive='receptor:ping',
            ttl=15
        ))
        return await self.send(message, expected_response)

    def find_shortest_path(self, to_node_id):
        """Implementation of Dijkstra algorithm"""
        cost_map = defaultdict(list)
        for left, right, cost in self._edges:
            cost_map[left].append((cost, right))
            cost_map[right].append((cost, left))

        heap, seen, mins = [(0, self.node_id, [])], set(), {self.node_id: 0}
        while heap:
            (cost, vertex, path) = heapq.heappop(heap)
            if vertex not in seen:
                seen.add(vertex)
                path = [vertex] + path
                if vertex == to_node_id:
                    logger.debug(f'Shortest path to {to_node_id} with cost {cost} is {path}')
                    return path
                cost_map_for_vertex = cost_map.get(vertex, ())
                random.shuffle(cost_map_for_vertex)
                for next_cost, next_vertex in cost_map.get(vertex, ()):
                    if next_vertex in seen:
                        continue
                    min_so_far = mins.get(next_vertex, None)
                    next_total_cost = cost + next_cost
                    if min_so_far is None or next_total_cost < min_so_far:
                        mins[next_vertex] = next_total_cost
                        heapq.heappush(heap, (next_total_cost, next_vertex, path))

    async def forward(self, msg, next_hop):
        """
        Forward a message on to the next hop closer to its destination
        """
        buffer_obj = self.receptor.buffer_mgr[next_hop]
        msg.header["route_list"].append(self.node_id)
        logger.debug(f'Forwarding frame {msg.msg_id} to {next_hop}')
        try:
            route_counter.inc()
            await buffer_obj.put(msg)
        except ReceptorBufferError as e:
            logger.exception("Receptor Buffer Write Error forwarding message to {}: {}".format(next_hop, e))
            # TODO: Possible to find another route? This might be a hard failure
        except Exception as e:
            logger.exception("Error trying to forward message to {}: {}".format(next_hop, e))

    def next_hop(self, recipient):
        """
        Return the node ID of the next hop for routing a message to the
        given recipient. If the current node is the recipient or there is
        no path, then return None.
        """
        if recipient == self.node_id:
            return None
        path = self.find_shortest_path(recipient)
        if path:
            return path[-2]

    async def send(self, message, expected_response=False):
        """
        Send a new message with the given outer envelope.
        """
        recipient = message.header["recipient"]
        next_node_id = self.next_hop(recipient)
        if not next_node_id:
            # TODO: This probably needs to emit an error response
            raise UnrouteableError(f'No route found to {recipient}')

        # TODO: Not signing/serializing in order to finish buffered output work

        message.header.update({
            "sender": self.node_id,
            "route_list": [self.node_id]
        })
        logger.debug(f'Sending {message.msg_id} to {recipient} via {next_node_id}')
        if expected_response and "directive" in message.header:
            self.response_registry[message.msg_id] = dict(message_sent_time=message.header["timestamp"])
        await self.forward(message, next_node_id)
