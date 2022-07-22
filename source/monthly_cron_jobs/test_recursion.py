

copied_to_list = dict()
copied_to_list[1] = [2,3,4]
copied_to_list[2] = [5,6]
copied_to_list[4] = [7]
copied_to_list[5] = [8,9]
copied_to_list[8] = [10]

#def build_up_return_list(cur_value,copied_to_list, return_list):
def build_up_return_list(cur_value,copied_to_list):
    print("In function : " + str(cur_value))
    if cur_value in copied_to_list:
        print("IN IF for : " + str(cur_value))
        for next_value in copied_to_list[cur_value]:
            #return_list.extend(build_up_return_list(next_value, copied_to_list, return_list))
            return_list.extend(build_up_return_list(next_value, copied_to_list))
    return 1


def build_child_objects_list(copied_to_list, master_list, last_iteration_list):
    next_iteration_list = list()
    #print("Last Iteration LIST:" + str(last_iteration_list))
    for obj_id in last_iteration_list:
        #print("Result of copied lookup for " + str(genome_ws_obj_id) + " : " + str(copied_to_lookup_dict.get(object_ws_obj_id)))
        if obj_id in copied_to_list:
            next_iteration_list = next_iteration_list + copied_to_list[obj_id]
    if len(next_iteration_list) > 0:
        #print("master list pre append: " + str(master_list))
        master_list = master_list + next_iteration_list
        master_list = build_child_objects_list(copied_to_list, master_list, next_iteration_list)
        #print("master list post function call: " + str(master_list))
    return master_list

def grow_copied_list(copied_to_lookup_dict, master_list, last_iteration_list):
    #grows the list of copied objects for a WS
    #returns the master_list and next iteration list
    next_iteration_list = list()
    #    print("Last Iteration LIST:" + str(last_iteration_list))
    for object_ws_obj_id in last_iteration_list:
        #        print("Result of copied lookup for " + str(genome_ws_obj_id) + " : " + str(copied_to_lookup_dict.get(object_ws_obj_id)))
        if object_ws_obj_id in copied_to_lookup_dict:
            next_iteration_list = next_iteration_list + copied_to_lookup_dict[object_ws_obj_id]
            #    print("Next Iteration LIST:" + str(next_iteration_list))
    if len(next_iteration_list) > 0:
        #        print("master list pre append: " + str(master_list))
        master_list = master_list + next_iteration_list
        #        print("master list post append: " + str(master_list))
        master_list = grow_copied_list(copied_to_lookup_dict, master_list, next_iteration_list)
        #        print("master list post function call: " + str(master_list))
    return master_list


#return_list = list()
#final_list = build_up_return_list(1,copied_to_list, [])
#final_list = build_up_return_list(1,copied_to_list)

#print("Final List: " + str(final_list))
#print("Return List: " + str(return_list))

lineage_list = [1]
lineage_list = build_child_objects_list(copied_to_list, lineage_list, [1])
print("Lineage List: " + str(lineage_list))


lineage_list2 = [1]
lineage_list2 = grow_copied_list(copied_to_list, lineage_list2, [1])
print("Lineage List2: " + str(lineage_list2))

lineage_list2a = [2]
#lineage_list2a = []
lineage_list2a = grow_copied_list(copied_to_list, lineage_list2a, [2])
print("Lineage List2a: " + str(lineage_list2a))


