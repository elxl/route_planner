import pandas as pd
import math

def hhmm_to_int(hhmm):
    '''
    Convert time string to minutes int
    '''
    tokens = hhmm.split(':')
    return int(tokens[0])*60 + int(tokens[1])

def to_hhmm(minutes):
    '''
    Convert minutes int to time string
    '''
    return '{:02}:{:02}'.format(minutes//60, minutes%60)

class Connection(object):
    
    def __init__(self, start_station, start_time, end_station, end_time, route_id=None, trip_id=None, delay=None):
        '''
        Static information for a connection from one station to the other with departure and arrival time
        '''
        self.start_station = start_station
        self.start_time = hhmm_to_int(start_time)
        self.end_station = end_station
        self.end_time = hhmm_to_int(end_time)
        self.route_id = route_id
        self.trip_id = trip_id
        self.delay = delay


class BaseProfile(object):

    class ProfileEntry(object):
        '''
        Each ProfileEntry instance is one entry in the itinerary list for one station
        '''
        
        def __init__(self, outgoing_conn, next_profile_entry, dept_time, arr_time, num_transfer, confidence):
            self.outgoing_conn = outgoing_conn
            self.next_profile_entry = next_profile_entry
            self.dept_time = hhmm_to_int(dept_time) if isinstance(dept_time, str) else dept_time
            self.arr_time = hhmm_to_int(arr_time) if isinstance(arr_time, str) else arr_time
            self.num_transfer = num_transfer
            self.confidence = confidence
            
              
        def dominatedBy(self, other):
            """ Returns whether self is dominated by other"""
            return other.dept_time >= self.dept_time\
                and other.confidence >= self.confidence\

        def __eq__(self, other):
            if other == None:
                return False

            return self.outgoing_conn == other.outgoing_conn and self.dept_time == other.dept_time\
             and self.arr_time == other.arr_time and self.num_transfer == other.num_transfer\
             and self.confidence == other.confidence

    def query(self, incoming_conn, transfer_time=0):
        raise NotImplementedError 
    

    def add_entry_if_not_dominated(self, new):
        raise NotImplementedError 


class IdentityProfile(BaseProfile):
    
    def __init__(self, deadline):
        """
        deadline: the latest arrival time set by the user. 
        """
        super().__init__()
        self.deadline = deadline
        

    def query(self, incoming_conn, transfer_time=0):
        
        if incoming_conn.end_time + transfer_time > self.deadline:
            return []
        
        return [BaseProfile.ProfileEntry(None, None, self.deadline, self.deadline, 0, 1)]

    def add_entry_if_not_dominated(self, new):
        pass


class Profile(BaseProfile):
    '''
    One instance of Profile is the itinerary for one station
    '''

    def __init__(self):
        self.entries = []
    
    def query(self, incoming_conn, transfer_time=0):
        '''
        Get possible sucessful transfer
        '''
        return list(filter(lambda x: x.outgoing_conn.trip_id == incoming_conn.trip_id or x.dept_time >= incoming_conn.end_time + transfer_time, self.entries))

    def add_entry_if_not_dominated(self, new):
        '''
        Add new entry to timetable, if `new` is not dominated by other entries.
        '''
        
        # First, check the existing ones at the end of the list with the same departure time as `new`.
        # If they are dominated by new, remove them.
        for tail in reversed(self.entries):
            if tail.dept_time > new.dept_time:
                break
            if tail.dominatedBy(new):
                self.entries.pop()
        
        # Then check if `new` is dominated by the last existing entry. 
        if len(self.entries) == 0 or not new.dominatedBy(self.entries[-1]):
            self.entries.append(new)
        
   
def generate_profiles(stations, dest_station, deadline):
    profiles = {}

    # read station list
    for s in stations:
        profiles[s] = Profile()
    
    # set dest_station
    profiles[dest_station] = IdentityProfile(deadline)

    return profiles

class Itinerary(object):
    '''
    Each instance of Itinerary is one entry for a query
    '''
    class Leg(object):
        def __init__(self, start_station, start_time, end_station, end_time, route_id=None, trip_id=None):
            self.start_station = start_station
            self.start_time = hhmm_to_int(start_time) if isinstance(start_time, str) else start_time
            self.end_station = end_station
            self.end_time = hhmm_to_int(end_time) if isinstance(end_time, str) else end_time
            self.route_id = route_id
            self.trip_id = trip_id
        
        def __str__(self):
            return f'{to_hhmm(self.start_time)} {self.start_station} ==({self.route_id} {self.trip_id})==> {to_hhmm(self.end_time)} {self.end_station}'
    
    class Waypoint(object):
        
        def __init__(self, station, time, route_id, trip_id):
            self.station = station
            self.time = time
            self.route_id = route_id
            self.trip_id = trip_id
        
        def to_dict(self, stop_id_to_name, stop_id_to_lonlat, route_id_to_name):
            lon, lat = stop_id_to_lonlat(self.station)
            return {
                'station': stop_id_to_name(self.station),
                'time': to_hhmm(self.time),
                'route': route_id_to_name(self.route_id),
                'trip_id': self.trip_id,
                'is_walking': self.route_id == None,
                'lat': lat,
                'lon': lon
            }
        
    
    
    def __init__(self, num_transfer, confidence):
        self.legs = []
        self.waypoints = []
        
        self.num_transfer = num_transfer
        self.confidence = confidence
    
    def add_leg(self, leg):
        self.legs.append(leg)
        
    def add_waypoint(self, stop, time, route_id, trip_id):
        self.waypoints.append(Itinerary.Waypoint(stop, time, route_id, trip_id))
    
    def waypoints_df(self, stop_id_to_name, stop_id_to_latlon, route_id_to_name):
        return pd.DataFrame.from_records([w.to_dict(stop_id_to_name, stop_id_to_latlon, route_id_to_name) for w in self.waypoints])
    
    def summary(self):
        return '{} => {}, {} transfer, {:.8f}%'.format(to_hhmm(self.legs[0].start_time), to_hhmm(self.legs[-1].end_time), self.num_transfer, float(self.confidence*100))
#         return f'{to_hhmm(self.legs[0].start_time)} => '\
#                  + f'{to_hhmm(self.legs[-1].end_time)}, {self.num_transfer}x transfers, {self.confidence*100}%'
    
    def __str__(self):
        return self.summary() + '\n' '\t' + '\n\t'.join([str(leg) for leg in self.legs]) + '\n'



def build_itinerary_from_profile_entry(profile_entry, start_station, end_station, nearby_station_dict):
    
    itinerary = Itinerary(profile_entry.num_transfer, profile_entry.confidence)
    
    if start_station != profile_entry.outgoing_conn.start_station:
        # If itinerary not starting from the user's query, add a walking section
        walk_from = start_station
        walk_start_time = profile_entry.dept_time
        walk_to = profile_entry.outgoing_conn.start_station
        walk_time = next(filter(lambda x: x[0] == walk_to, nearby_station_dict[walk_from]))[1]
        itinerary.add_leg(
            Itinerary.Leg(walk_from, walk_start_time , walk_to, walk_start_time + walk_time, route_id=None, trip_id=None)
        )
        
        # Add walking waypoints
        itinerary.add_waypoint(walk_from, walk_start_time, route_id=None, trip_id=None)
        itinerary.add_waypoint(walk_to, walk_start_time + walk_time, route_id=None, trip_id=None)
        
        

    prev_boarding_station = profile_entry.outgoing_conn.start_station
    prev_boarding_time = profile_entry.outgoing_conn.start_time
    
    # add start station as waypoint
    itinerary.add_waypoint(prev_boarding_station,
                           prev_boarding_time,
                           route_id=profile_entry.outgoing_conn.route_id,
                           trip_id=profile_entry.outgoing_conn.trip_id)
    
    current_profile_entry = profile_entry

    while True:
        next_profile_entry = current_profile_entry.next_profile_entry
        if next_profile_entry.outgoing_conn == None:
            break
        
        # add connection end station as waypoint
        itinerary.add_waypoint(
            current_profile_entry.outgoing_conn.end_station,
            current_profile_entry.outgoing_conn.end_time,
            current_profile_entry.outgoing_conn.route_id,
            current_profile_entry.outgoing_conn.trip_id)
        
        if next_profile_entry.outgoing_conn.trip_id != current_profile_entry.outgoing_conn.trip_id:
            # Transfer
            itinerary.add_leg(
                Itinerary.Leg(prev_boarding_station, prev_boarding_time,\
                    current_profile_entry.outgoing_conn.end_station,\
                    current_profile_entry.outgoing_conn.end_time,
                    route_id=current_profile_entry.outgoing_conn.route_id,
                    trip_id=current_profile_entry.outgoing_conn.trip_id))

            prev_boarding_station = next_profile_entry.outgoing_conn.start_station
            prev_boarding_time = next_profile_entry.outgoing_conn.start_time
            
            
            if next_profile_entry.outgoing_conn.start_station != current_profile_entry.outgoing_conn.end_station:
                # add Walking leg if the end station of the current conn differs from the start station of the next conn
                walk_from = current_profile_entry.outgoing_conn.end_station
                walk_to = next_profile_entry.outgoing_conn.start_station
                walk_start_time = current_profile_entry.outgoing_conn.end_time
                walk_time = next(filter(lambda x: x[0] == walk_to, nearby_station_dict[walk_from]))[1]
                
                itinerary.add_leg(Itinerary.Leg(walk_from, walk_start_time, walk_to, walk_start_time + walk_time, route_id=None, trip_id=None)
                )
                
                # Start of walking section
                itinerary.add_waypoint(walk_from, walk_start_time, route_id=None, trip_id=None)
                # End of walking section
                itinerary.add_waypoint(walk_to, walk_start_time + walk_time, route_id=None, trip_id=None)
            
            # Add start of next train
            itinerary.add_waypoint(
                next_profile_entry.outgoing_conn.start_station,
                next_profile_entry.outgoing_conn.start_time,
                route_id=next_profile_entry.outgoing_conn.route_id,
                trip_id=next_profile_entry.outgoing_conn.trip_id)
                

        current_profile_entry = next_profile_entry
    
    # Add Final Leg
    itinerary.add_leg(
            Itinerary.Leg(prev_boarding_station, prev_boarding_time,\
                current_profile_entry.outgoing_conn.end_station,\
                current_profile_entry.outgoing_conn.end_time,
                route_id=current_profile_entry.outgoing_conn.route_id,
                trip_id=current_profile_entry.outgoing_conn.trip_id))
    
    # Add final train waypoint
    itinerary.add_waypoint(
        current_profile_entry.outgoing_conn.end_station,
        current_profile_entry.outgoing_conn.end_time, 
        route_id=current_profile_entry.outgoing_conn.route_id,
        trip_id=current_profile_entry.outgoing_conn.trip_id
    )
    
    
    # Add final walking section if the alighting station differs from the destination station
    if end_station != current_profile_entry.outgoing_conn.end_station:
        walk_from =  current_profile_entry.outgoing_conn.end_station
        walk_start_time = current_profile_entry.outgoing_conn.end_time
        walk_time = next(filter(lambda x: x[0] == end_station, nearby_station_dict[walk_from]))[1]
        
        itinerary.add_leg(
            Itinerary.Leg(walk_from, walk_start_time, end_station, walk_start_time + walk_time, route_id=None, trip_id=None)
        )
        
        # Add walking waypoints
        itinerary.add_waypoint(walk_from, walk_start_time, route_id=None, trip_id=None)
        itinerary.add_waypoint(end_station, walk_start_time+walk_time, route_id=None, trip_id=None)


    return itinerary


def csa(stations, connections, origin_station, destination_station, deadline, confidence_bound, nearby_station_dic):
    '''
    Entry in nearby_station_dic: station_id:(station_id, walking_time), platform name is not included
    '''
    profiles = generate_profiles(stations, destination_station, deadline)
    connections.sort(key=lambda c: (c.start_time, -c.end_time), reverse=True)

    for conn in connections:
        downstreams = profiles[conn.end_station].query(conn)
        # merge conn with downstreams
        update_candidates = []
        entries = extend_entries(conn, downstreams, confidence_bound)
        update_candidates.extend(entries)
        for near_station in nearby_station_dic[conn.end_station]:
            if near_station[0] in stations:
                walking_time = near_station[1]
                downstreams = profiles[near_station[0]].query(conn, transfer_time=walking_time)
                # merge conn with downstreams
                entries = extend_entries(conn, downstreams, confidence_bound, transfer_time=walking_time, walking=True)
                update_candidates.extend(entries)
        
        # Since all entries in `pending_updates` have the same departure time, we only need to find the 
        # one with the highest confidence (break the tie by the number of transfers).
        
        if len(update_candidates) > 0:
            best = max(update_candidates, key=lambda x: (x.confidence, -x.num_transfer, -x.arr_time))
            profiles[conn.start_station].add_entry_if_not_dominated(best)
    
    
    
    return merge_neighboring_start_station_entries(origin_station, profiles, nearby_station_dic)
    

def merge_neighboring_start_station_entries(origin_station, profiles, nearby_dict):
    
    candidates = profiles[origin_station].entries
    
    for nearby_stop, walk_time in nearby_dict[origin_station]:
        if nearby_stop not in profiles:
            continue
        for entry in profiles[nearby_stop].entries:
            entry.dept_time -= walk_time
        candidates.extend(profiles[nearby_stop].entries)
    
    # remove dominated entries
    candidates.sort(key=lambda x: (-x.dept_time, -x.confidence, x.num_transfer))

    results = []
    for c in candidates:
        if len(results) == 0 or not c.dominatedBy(results[-1]):
            results.append(c)

    
    return results


def extend_entries(conn, downstreams, confidence_bound, calc_confidence=True, transfer_time = 2, walking=False):
    def predict_confidence(conn, prev_train_arr_time, next_train_dept_time, mct):
        """
        Return the probability that there's sufficient time for transfering. 
        mct: minimum connection time (i.e. the minimum walking time required for the transfer)
        """
        # TODO: replace with real confidence
#         assert(next_train_dept_time - prev_train_arr_time >= mct)
#         return min(1, .75 + .03 * (next_train_dept_time - prev_train_arr_time - mct))
        # no delay information for the station
        
        allowed_delay = next_train_dept_time - prev_train_arr_time -mct
        if allowed_delay>10:
            # delay more than 10 minutes
            return 1
        else:
            return conn.delay[allowed_delay]
            


    result = []

    for ds in downstreams:
        new_dept_time = conn.start_time
        new_arr_time = ds.arr_time

        is_transfering = ds.outgoing_conn != None and conn.trip_id != ds.outgoing_conn.trip_id 
        is_reaching_dest = ds.outgoing_conn == None
       
        num_transfer = ds.num_transfer + 1 if is_transfering else ds.num_transfer
        
        if not is_transfering:
            if is_reaching_dest: # Consider the delay of the final leg.
                confidence = predict_confidence(conn, conn.end_time, ds.arr_time, transfer_time if walking else 0) # walking is true when the user needs to walk to the destination station.
                if confidence > confidence_bound:
                    result.append(BaseProfile.ProfileEntry(conn, ds, new_dept_time, new_arr_time, num_transfer, confidence))
            else:
                result.append(BaseProfile.ProfileEntry(conn, ds, new_dept_time, new_arr_time, num_transfer, ds.confidence))
            
        elif ds.outgoing_conn.start_time >= conn.end_time + transfer_time:
            new_confidence = ds.confidence * predict_confidence(conn, conn.end_time, ds.outgoing_conn.start_time, transfer_time) if calc_confidence else ds.confidence
            if new_confidence > confidence_bound:
                result.append(BaseProfile.ProfileEntry(conn, ds, new_dept_time, new_arr_time, num_transfer, new_confidence))
    
    return result
