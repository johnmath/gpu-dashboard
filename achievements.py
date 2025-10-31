"""
Achievement System for GPU Dashboard

This module defines achievements related to GPU, CPU, and memory usage.
Achievements are tracked per canonical user and stored in achievements.json.
"""

from datetime import datetime
from typing import Dict, List, Set, Tuple
import json
import os

# Achievement definitions
ACHIEVEMENTS = {
    # GPU Achievements
    "quad_gpu_master": {
        "name": "Quad GPU Master",
        "description": "Use 4 or more GPUs simultaneously",
        "icon": "🎯",
        "tier": "gold"
    },
    "gpu_hoarder": {
        "name": "GPU Hoarder",
        "description": "Use 8 or more GPUs simultaneously",
        "icon": "💎",
        "tier": "platinum"
    },
    "memory_titan": {
        "name": "Memory Titan",
        "description": "Achieve >= 90% memory utilization on a single GPU",
        "icon": "🏔️",
        "tier": "gold"
    },
    "memory_perfectionist": {
        "name": "Memory Perfectionist",
        "description": "Achieve >= 99% memory utilization on a single GPU",
        "icon": "💯",
        "tier": "platinum"
    },
    "utilization_champion": {
        "name": "Utilization Champion",
        "description": "Achieve >= 95% GPU utilization",
        "icon": "⚡",
        "tier": "gold"
    },
    "gpu_marathon": {
        "name": "GPU Marathon",
        "description": "Run a process for over 24 hours on a GPU",
        "icon": "🏃",
        "tier": "silver"
    },
    "gpu_ultra_marathon": {
        "name": "GPU Ultra Marathon",
        "description": "Run a process for over 7 days on a GPU",
        "icon": "🏃‍♂️💨",
        "tier": "gold"
    },
    
    # Memory Achievements
    "ram_beast": {
        "name": "RAM Beast",
        "description": "Use more than 300GB RAM at once",
        "icon": "🐂",
        "tier": "gold"
    },
    "ram_monster": {
        "name": "RAM Monster",
        "description": "Use more than 500GB RAM at once",
        "icon": "👹",
        "tier": "platinum"
    },
    
    # CPU Achievements
    "cpu_maximus": {
        "name": "CPU Maximus",
        "description": "Use all CPU cores (>95% CPU utilization)",
        "icon": "🔥",
        "tier": "gold"
    },
    
    # Multi-Machine Achievements
    "cluster_commander": {
        "name": "Cluster Commander",
        "description": "Use GPUs on 3 or more different machines simultaneously",
        "icon": "🎖️",
        "tier": "gold"
    },
    "cluster_overlord": {
        "name": "Cluster Overlord",
        "description": "Use GPUs on 5 or more different machines simultaneously",
        "icon": "👑",
        "tier": "platinum"
    },
    
    # Lifetime Achievements
    "gpu_veteran": {
        "name": "GPU Veteran",
        "description": "Accumulate 100 GB-Hours of GPU usage",
        "icon": "🎖️",
        "tier": "silver"
    },
    "gpu_hero": {
        "name": "GPU Hero",
        "description": "Accumulate 1,000 GB-Hours of GPU usage",
        "icon": "🦸",
        "tier": "gold"
    },
    "gpu_legend": {
        "name": "GPU Legend",
        "description": "Accumulate 10,000 GB-Hours of GPU usage",
        "icon": "⭐",
        "tier": "platinum"
    },
    
    # Coop Achievements
    "gpu_roommate": {
        "name": "GPU Roommate",
        "description": "Share a GPU with another user",
        "icon": "🤝",
        "tier": "bronze"
    },
    "party_machine": {
        "name": "Party Machine",
        "description": "Have 4 or more different users using GPUs on the same machine",
        "icon": "🎉",
        "tier": "gold"
    },
    "full_house": {
        "name": "Full House",
        "description": "Have all GPUs on a machine occupied by different users",
        "icon": "🏠",
        "tier": "gold"
    },
    
    # Milestone Achievements
    "first_blood": {
        "name": "First Blood",
        "description": "Use your first GPU",
        "icon": "🩸",
        "tier": "bronze"
    },
    "globe_trotter": {
        "name": "Globe Trotter",
        "description": "Use GPUs on 10 different machines (lifetime)",
        "icon": "🌍",
        "tier": "platinum"
    },
    "efficiency_expert": {
        "name": "Efficiency Expert",
        "description": "Maintain >80% GPU utilization across all your active GPUs",
        "icon": "📊",
        "tier": "gold"
    },
}


def load_achievements(path: str) -> Dict:
    """Load existing achievements from file."""
    if not os.path.exists(path):
        return {"users": {}, "updated_at": None}
    
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: failed to load achievements from {path}: {e}")
        return {"users": {}, "updated_at": None}


def save_achievements(path: str, data: Dict):
    """Save achievements to file."""
    try:
        data["updated_at"] = datetime.utcnow().isoformat() + "Z"
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Warning: failed to save achievements to {path}: {e}")


def parse_elapsed_time(time_str: str) -> float:
    """
    Parse elapsed time string (e.g., '12:34:56', '1-02:34:56') to hours.
    Returns hours as float.
    """
    try:
        # Handle format like '1-02:34:56' (days-hours:minutes:seconds)
        if '-' in time_str:
            parts = time_str.split('-')
            days = int(parts[0])
            time_part = parts[1]
        else:
            days = 0
            time_part = time_str
        
        # Parse hours:minutes:seconds
        time_components = time_part.split(':')
        if len(time_components) == 3:
            hours = int(time_components[0])
            minutes = int(time_components[1])
            seconds = int(time_components[2])
        else:
            return 0.0
        
        total_hours = days * 24 + hours + minutes / 60 + seconds / 3600
        return total_hours
    except:
        return 0.0


def check_achievements(
    all_stats: Dict,
    aggregate_stats: Dict,
    alias_map: Dict,
    achievements_data: Dict
) -> Tuple[Dict, List[Dict]]:
    """
    Check for new achievements based on current snapshot and aggregate stats.
    
    Returns:
        Tuple of (updated_achievements_data, list_of_new_achievements)
        where new_achievements is a list of dicts with keys:
            - user: canonical username
            - achievement_id: achievement key
            - achievement: achievement definition
            - timestamp: when it was earned
    """
    now = datetime.utcnow().isoformat() + "Z"
    new_achievements = []
    
    # Ensure users dict exists
    if "users" not in achievements_data:
        achievements_data["users"] = {}
    
    # Helper to canonicalize usernames
    def canonicalize(username):
        if not username:
            return "unknown"
        return alias_map.get(username.lower(), username)
    
    # Collect snapshot data per user
    user_snapshot = {}  # canonical_user -> stats
    
    for server in all_stats.get("servers", []):
        if server.get("error"):
            continue
        
        server_name = server.get("name", "unknown")
        cpu_util = server.get("cpu_util", 0)
        
        # Track users per server for coop achievements
        server_users = set()
        gpu_users = {}  # gpu_index -> set of users
        
        for gpu in server.get("gpus", []):
            gpu_index = gpu.get("index", 0)
            gpu_users[gpu_index] = set()
            
            for proc in gpu.get("processes", []):
                raw_user = proc.get("user") or "unknown"
                if raw_user == "root":
                    continue
                
                canonical = canonicalize(raw_user)
                
                server_users.add(canonical)
                gpu_users[gpu_index].add(canonical)
                
                if canonical not in user_snapshot:
                    user_snapshot[canonical] = {
                        "gpu_count": 0,
                        "machines": set(),
                        "total_mem_mb": 0,
                        "max_gpu_mem_percent": 0,
                        "max_gpu_util": 0,
                        "max_process_hours": 0,
                        "cpu_machines": set(),
                        "gpu_indices": set(),  # Track unique GPUs
                    }
                
                stats = user_snapshot[canonical]
                stats["gpu_indices"].add(f"{server_name}:{gpu_index}")
                stats["machines"].add(server_name)
                stats["total_mem_mb"] += proc.get("mem", 0)
                
                # Check GPU stats
                mem_used = gpu.get("mem_used", 0)
                mem_total = gpu.get("mem_total", 1)
                mem_percent = (mem_used / mem_total) * 100 if mem_total > 0 else 0
                stats["max_gpu_mem_percent"] = max(stats["max_gpu_mem_percent"], mem_percent)
                stats["max_gpu_util"] = max(stats["max_gpu_util"], gpu.get("util", 0))
                
                # Check process runtime
                proc_time = proc.get("time", "0:00:00")
                proc_hours = parse_elapsed_time(proc_time)
                stats["max_process_hours"] = max(stats["max_process_hours"], proc_hours)
        
        # Check CPU usage for this user on this machine
        if cpu_util > 95:
            for user in server_users:
                if user in user_snapshot:
                    user_snapshot[user]["cpu_machines"].add(server_name)
        
        # Check coop achievements per server
        if len(server_users) >= 4:
            # Party Machine achievement
            for user in server_users:
                award_achievement(
                    achievements_data, user, "party_machine",
                    now, new_achievements
                )
        
        # Full House: all GPUs occupied by different users
        if len(gpu_users) > 0:
            total_gpus = len(gpu_users)
            occupied_gpus = sum(1 for users in gpu_users.values() if len(users) > 0)
            if occupied_gpus == total_gpus and occupied_gpus > 1:
                # Check if each GPU has different users
                all_users_per_gpu = [users for users in gpu_users.values() if len(users) > 0]
                if len(all_users_per_gpu) > 1:
                    # Award to all participants
                    for users in all_users_per_gpu:
                        for user in users:
                            award_achievement(
                                achievements_data, user, "full_house",
                                now, new_achievements
                            )
        
        # GPU Roommate: sharing a GPU
        for gpu_index, users in gpu_users.items():
            if len(users) >= 2:
                for user in users:
                    award_achievement(
                        achievements_data, user, "gpu_roommate",
                        now, new_achievements
                    )
    
    # Now update GPU counts based on unique GPU indices
    for user, stats in user_snapshot.items():
        stats["gpu_count"] = len(stats["gpu_indices"])
    
    # Check individual achievements for each user
    for user, stats in user_snapshot.items():
        # First Blood
        award_achievement(achievements_data, user, "first_blood", now, new_achievements)
        
        # GPU count achievements
        if stats["gpu_count"] >= 4:
            award_achievement(achievements_data, user, "quad_gpu_master", now, new_achievements)
        if stats["gpu_count"] >= 8:
            award_achievement(achievements_data, user, "gpu_hoarder", now, new_achievements)
        
        # Memory achievements
        total_mem_gb = stats["total_mem_mb"] / 1024
        if total_mem_gb >= 300:
            award_achievement(achievements_data, user, "ram_beast", now, new_achievements)
        if total_mem_gb >= 500:
            award_achievement(achievements_data, user, "ram_monster", now, new_achievements)
        
        # GPU memory utilization
        if stats["max_gpu_mem_percent"] >= 90:
            award_achievement(achievements_data, user, "memory_titan", now, new_achievements)
        if stats["max_gpu_mem_percent"] >= 99:
            award_achievement(achievements_data, user, "memory_perfectionist", now, new_achievements)
        
        # GPU utilization
        if stats["max_gpu_util"] >= 95:
            award_achievement(achievements_data, user, "utilization_champion", now, new_achievements)
        
        # Process runtime
        if stats["max_process_hours"] >= 24:
            award_achievement(achievements_data, user, "gpu_marathon", now, new_achievements)
        if stats["max_process_hours"] >= 168:  # 7 days
            award_achievement(achievements_data, user, "gpu_ultra_marathon", now, new_achievements)
        
        # CPU achievements
        if len(stats["cpu_machines"]) > 0:
            award_achievement(achievements_data, user, "cpu_maximus", now, new_achievements)
        
        # Multi-machine achievements
        if len(stats["machines"]) >= 3:
            award_achievement(achievements_data, user, "cluster_commander", now, new_achievements)
        if len(stats["machines"]) >= 5:
            award_achievement(achievements_data, user, "cluster_overlord", now, new_achievements)
        
        # Efficiency expert: >80% util across all active GPUs
        # (simplified check - would need more detailed tracking)
        if stats["gpu_count"] > 0 and stats["max_gpu_util"] >= 80:
            award_achievement(achievements_data, user, "efficiency_expert", now, new_achievements)
    
    # Check lifetime achievements from aggregate stats
    if aggregate_stats and "users" in aggregate_stats:
        for raw_user, agg_stats in aggregate_stats["users"].items():
            # Canonicalize the user from aggregate stats too
            user = canonicalize(raw_user)
            
            gb_hours = agg_stats.get("total_gb_hours", 0)
            
            if gb_hours >= 100:
                award_achievement(achievements_data, user, "gpu_veteran", now, new_achievements)
            if gb_hours >= 1000:
                award_achievement(achievements_data, user, "gpu_hero", now, new_achievements)
            if gb_hours >= 10000:
                award_achievement(achievements_data, user, "gpu_legend", now, new_achievements)
            
            # Globe Trotter: 10 different machines lifetime
            all_machines = set(agg_stats.get("all_machines", []))
            if len(all_machines) >= 10:
                award_achievement(achievements_data, user, "globe_trotter", now, new_achievements)
    
    return achievements_data, new_achievements


def award_achievement(
    achievements_data: Dict,
    user: str,
    achievement_id: str,
    timestamp: str,
    new_achievements_list: List[Dict]
):
    """Award an achievement to a user if they don't already have it."""
    if user not in achievements_data["users"]:
        achievements_data["users"][user] = {}
    
    user_achievements = achievements_data["users"][user]
    
    if achievement_id not in user_achievements:
        user_achievements[achievement_id] = {
            "earned_at": timestamp,
            "achievement": ACHIEVEMENTS.get(achievement_id, {})
        }
        
        # Add to new achievements list for logging
        new_achievements_list.append({
            "user": user,
            "achievement_id": achievement_id,
            "achievement": ACHIEVEMENTS.get(achievement_id, {}),
            "timestamp": timestamp
        })


def get_user_achievements(achievements_data: Dict, user: str) -> List[Dict]:
    """Get all achievements for a user, sorted by tier and earned date."""
    if "users" not in achievements_data or user not in achievements_data["users"]:
        return []
    
    user_achvs = achievements_data["users"][user]
    
    tier_order = {"platinum": 0, "gold": 1, "silver": 2, "bronze": 3}
    
    achievements_list = []
    for achv_id, achv_data in user_achvs.items():
        achv_def = ACHIEVEMENTS.get(achv_id, {})
        achievements_list.append({
            "id": achv_id,
            "name": achv_def.get("name", "Unknown"),
            "description": achv_def.get("description", ""),
            "icon": achv_def.get("icon", "🏆"),
            "tier": achv_def.get("tier", "bronze"),
            "earned_at": achv_data.get("earned_at", "")
        })
    
    achievements_list.sort(key=lambda x: (tier_order.get(x["tier"], 99), x["earned_at"]))
    
    return achievements_list


def get_achievement_stats(achievements_data: Dict) -> Dict:
    """Get overall achievement statistics."""
    stats = {
        "total_achievements_earned": 0,
        "total_users_with_achievements": 0,
        "achievement_distribution": {},
        "top_achievers": []
    }
    
    if "users" not in achievements_data:
        return stats
    
    # Count achievements
    achievement_counts = {}
    user_counts = []
    
    for user, user_achvs in achievements_data["users"].items():
        count = len(user_achvs)
        user_counts.append({"user": user, "count": count})
        stats["total_achievements_earned"] += count
        
        for achv_id in user_achvs.keys():
            achievement_counts[achv_id] = achievement_counts.get(achv_id, 0) + 1
    
    stats["total_users_with_achievements"] = len(achievements_data["users"])
    stats["achievement_distribution"] = achievement_counts
    
    # Top achievers
    user_counts.sort(key=lambda x: x["count"], reverse=True)
    stats["top_achievers"] = user_counts[:10]
    
    return stats
