#include <nav_msgs/Odometry.h>
#include <nav_msgs/Path.h>
#include <pcl_conversions/pcl_conversions.h>
#include <ros/ros.h>
#include <sensor_msgs/PointCloud2.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "include/btc.h"
#include "include/utils.h"

// Helper functions for saving intermediate results
void save_point_cloud_txt(const pcl::PointCloud<pcl::PointXYZI> &cloud, 
                          const std::string &filename) {
  std::ofstream ofs(filename);
  ofs << "# x y z intensity" << std::endl;
  for (const auto &point : cloud.points) {
    ofs << std::fixed << std::setprecision(6) 
        << point.x << " " << point.y << " " << point.z << " " << point.intensity << std::endl;
  }
  ofs.close();
}

void save_planes_txt(const pcl::PointCloud<pcl::PointXYZINormal> &planes, 
                     const std::string &filename) {
  std::ofstream ofs(filename);
  ofs << "# Planes" << std::endl;
  ofs << "# center_x center_y center_z normal_x normal_y normal_z" << std::endl;
  for (const auto &plane : planes.points) {
    ofs << std::fixed << std::setprecision(6)
        << plane.x << " " << plane.y << " " << plane.z << " "
        << plane.normal_x << " " << plane.normal_y << " " << plane.normal_z << std::endl;
  }
  ofs.close();
}

void save_binary_descriptors_txt(const std::vector<BinaryDescriptor> &binary_list, 
                                  const std::string &filename) {
  std::ofstream ofs(filename);
  ofs << "# Binary Descriptors" << std::endl;
  ofs << "# location_x location_y location_z summary occupy_array_length occupy_array" << std::endl;
  for (const auto &bd : binary_list) {
    ofs << std::fixed << std::setprecision(6)
        << bd.location_[0] << " " << bd.location_[1] << " " << bd.location_[2] << " "
        << (int)bd.summary_ << " " << bd.occupy_array_.size() << " ";
    for (bool bit : bd.occupy_array_) {
      ofs << (bit ? "1" : "0");
    }
    ofs << std::endl;
  }
  ofs.close();
}

void save_btc_descriptors_txt(const std::vector<BTC> &btc_list, 
                              const std::string &filename) {
  std::ofstream ofs(filename);
  ofs << "# BTC Descriptors" << std::endl;
  ofs << "# triangle_x triangle_y triangle_z center_x center_y center_z frame_number" << std::endl;
  ofs << "# binary_A_loc_x binary_A_loc_y binary_A_loc_z binary_A_summary" << std::endl;
  ofs << "# binary_B_loc_x binary_B_loc_y binary_B_loc_z binary_B_summary" << std::endl;
  ofs << "# binary_C_loc_x binary_C_loc_y binary_C_loc_z binary_C_summary" << std::endl;
  for (const auto &btc : btc_list) {
    ofs << std::fixed << std::setprecision(6)
        << btc.triangle_[0] << " " << btc.triangle_[1] << " " << btc.triangle_[2] << " "
        << btc.center_[0] << " " << btc.center_[1] << " " << btc.center_[2] << " "
        << btc.frame_number_ << std::endl;
    ofs << btc.binary_A_.location_[0] << " " << btc.binary_A_.location_[1] << " "
        << btc.binary_A_.location_[2] << " " << (int)btc.binary_A_.summary_ << std::endl;
    ofs << btc.binary_B_.location_[0] << " " << btc.binary_B_.location_[1] << " "
        << btc.binary_B_.location_[2] << " " << (int)btc.binary_B_.summary_ << std::endl;
    ofs << btc.binary_C_.location_[0] << " " << btc.binary_C_.location_[1] << " "
        << btc.binary_C_.location_[2] << " " << (int)btc.binary_C_.summary_ << std::endl;
  }
  ofs.close();
}

void save_stage_statistics(int frame_id, const std::string &stage, 
                          const std::map<std::string, double> &stats,
                          const std::string &output_dir) {
  std::stringstream ss;
  ss << output_dir << "/frame_" << std::setfill('0') << std::setw(6) << frame_id 
     << "_stage_" << stage << ".json";
  
  std::ofstream ofs(ss.str());
  ofs << "{" << std::endl;
  ofs << "  \"frame_id\": " << frame_id << "," << std::endl;
  ofs << "  \"stage\": \"" << stage << "\"," << std::endl;
  
  for (auto it = stats.begin(); it != stats.end(); ++it) {
    ofs << "  \"" << it->first << "\": " << std::fixed << std::setprecision(6) << it->second;
    if (std::next(it) != stats.end()) ofs << ",";
    ofs << std::endl;
  }
  
  ofs << "}" << std::endl;
  ofs.close();
}

void save_configuration(const ConfigSetting &config_setting, const std::string &output_dir) {
  std::string filename = output_dir + "/frame_-00001_stage_config.json";
  std::ofstream ofs(filename);
  
  ofs << "{" << std::endl;
  ofs << "  \"voxel_size_\": " << config_setting.voxel_size_ << "," << std::endl;
  ofs << "  \"useful_corner_num_\": " << config_setting.useful_corner_num_ << "," << std::endl;
  ofs << "  \"plane_detection_thre_\": " << config_setting.plane_detection_thre_ << "," << std::endl;
  ofs << "  \"proj_plane_num_\": " << config_setting.proj_plane_num_ << "," << std::endl;
  ofs << "  \"proj_image_resolution_\": " << config_setting.proj_image_resolution_ << "," << std::endl;
  ofs << "  \"similarity_threshold_\": " << config_setting.similarity_threshold_ << "," << std::endl;
  ofs << "  \"skip_near_num_\": " << config_setting.skip_near_num_ << "," << std::endl;
  ofs << "  \"descriptor_near_num_\": " << config_setting.descriptor_near_num_ << "," << std::endl;
  ofs << "  \"descriptor_min_len_\": " << config_setting.descriptor_min_len_ << "," << std::endl;
  ofs << "  \"descriptor_max_len_\": " << config_setting.descriptor_max_len_ << "," << std::endl;
  ofs << "  \"plane_merge_normal_thre_\": " << config_setting.plane_merge_normal_thre_ << "," << std::endl;
  ofs << "  \"plane_merge_dis_thre_\": " << config_setting.plane_merge_dis_thre_ << std::endl;
  ofs << "}" << std::endl;
  
  ofs.close();
}

// Read KITTI data
std::vector<float> read_lidar_data(const std::string lidar_data_path) {
  std::ifstream lidar_data_file;
  lidar_data_file.open(lidar_data_path,
                       std::ifstream::in | std::ifstream::binary);
  if (!lidar_data_file) {
    std::cout << "Read End..." << std::endl;
    std::vector<float> nan_data;
    return nan_data;
  }
  lidar_data_file.seekg(0, std::ios::end);
  const size_t num_elements = lidar_data_file.tellg() / sizeof(float);
  lidar_data_file.seekg(0, std::ios::beg);

  std::vector<float> lidar_data_buffer(num_elements);
  lidar_data_file.read(reinterpret_cast<char *>(&lidar_data_buffer[0]),
                       num_elements * sizeof(float));
  return lidar_data_buffer;
}

int main(int argc, char **argv) {
  ros::init(argc, argv, "btc_place_recognition_verification");
  ros::NodeHandle nh;
  
  // Parameters
  std::string setting_path = "";
  std::string pcds_dir = "";
  std::string pose_file = "";
  std::string output_dir = "cpp_verification_results";
  double cloud_overlap_thr = 0.5;
  bool calc_gt_enable = false;
  bool read_bin = true;
  int max_frames = 50;
  
  nh.param<double>("cloud_overlap_thr", cloud_overlap_thr, 0.5);
  nh.param<std::string>("setting_path", setting_path, "");
  nh.param<std::string>("pcds_dir", pcds_dir, "");
  nh.param<std::string>("pose_file", pose_file, "");
  nh.param<std::string>("output_dir", output_dir, "cpp_verification_results");
  nh.param<bool>("read_bin", read_bin, true);
  nh.param<int>("max_frames", max_frames, 50);

  // Create output directory
  boost::filesystem::create_directories(output_dir);
  
  std::cout << "Starting BTC C++ verification with intermediate result saving..." << std::endl;
  std::cout << "Output directory: " << output_dir << std::endl;

  // ROS Publishers (kept for compatibility)
  ros::Publisher pubOdomAftMapped =
      nh.advertise<nav_msgs::Odometry>("/aft_mapped_to_init", 10);
  ros::Publisher pubCureentCloud =
      nh.advertise<sensor_msgs::PointCloud2>("/cloud_current", 100);
  ros::Publisher pubCurrentBinary =
      nh.advertise<sensor_msgs::PointCloud2>("/cloud_key_points", 100);
  ros::Publisher pubPath =
      nh.advertise<visualization_msgs::MarkerArray>("descriptor_line", 10);
  ros::Publisher pubCurrentPose =
      nh.advertise<nav_msgs::Odometry>("/current_pose", 10);
  ros::Publisher pubMatchedPose =
      nh.advertise<nav_msgs::Odometry>("/matched_pose", 10);
  ros::Publisher pubMatchedCloud =
      nh.advertise<sensor_msgs::PointCloud2>("/cloud_matched", 100);
  ros::Publisher pubMatchedBinary =
      nh.advertise<sensor_msgs::PointCloud2>("/cloud_matched_key_points", 100);
  ros::Publisher pubLoopStatus =
      nh.advertise<visualization_msgs::MarkerArray>("/loop_status", 100);
  ros::Publisher pubBTC =
      nh.advertise<visualization_msgs::MarkerArray>("descriptor_line", 10);

  // Load configuration
  ConfigSetting config_setting;
  load_config_setting(setting_path, config_setting);
  
  // Save configuration
  save_configuration(config_setting, output_dir);

  // Load poses for visualization and gt overlap calculation
  std::vector<std::pair<Eigen::Vector3d, Eigen::Matrix3d>> pose_list;
  std::vector<double> time_list;
  load_evo_pose_with_time(pose_file, pose_list, time_list);
  std::string print_msg = "Successfully load pose file:" + pose_file +
                          ". pose size:" + std::to_string(time_list.size());
  ROS_INFO_STREAM(print_msg.c_str());
  
  // Save poses for verification
  std::string poses_file = output_dir + "/frame_-00001_stage_poses.json";
  std::ofstream poses_ofs(poses_file);
  poses_ofs << "[" << std::endl;
  for (size_t i = 0; i < std::min(pose_list.size(), (size_t)max_frames); i++) {
    poses_ofs << "  {" << std::endl;
    poses_ofs << "    \"frame_id\": " << i << "," << std::endl;
    poses_ofs << "    \"translation\": [" << pose_list[i].first.transpose() << "]," << std::endl;
    poses_ofs << "    \"rotation\": [" << std::endl;
    for (int row = 0; row < 3; row++) {
      poses_ofs << "      [";
      for (int col = 0; col < 3; col++) {
        poses_ofs << pose_list[i].second(row, col);
        if (col < 2) poses_ofs << ", ";
      }
      poses_ofs << "]";
      if (row < 2) poses_ofs << ",";
      poses_ofs << std::endl;
    }
    poses_ofs << "    ]" << std::endl;
    poses_ofs << "  }";
    if (i < std::min(pose_list.size(), (size_t)max_frames) - 1) poses_ofs << ",";
    poses_ofs << std::endl;
  }
  poses_ofs << "]" << std::endl;
  poses_ofs.close();

  BtcDescManager *btc_manager = new BtcDescManager(config_setting);
  btc_manager->print_debug_info_ = true;
  pcl::PointCloud<pcl::PointXYZI>::Ptr temp_cloud(
      new pcl::PointCloud<pcl::PointXYZI>());

  std::vector<double> descriptor_time;
  std::vector<double> querying_time;
  std::vector<double> update_time;
  std::vector<std::tuple<int, int, double>> trigger_loop_list;
  std::vector<std::tuple<int, int, double>> true_loop_list;
  std::vector<std::tuple<int, int, double>> false_loop_list;
  
  int triggle_loop_num = 0;
  int true_loop_num = 0;
  bool finish = false;

  pcl::PCDReader reader;
  ros::Rate loop(50000);
  ros::Rate slow_loop(1000);

  size_t process_frames = std::min(pose_list.size(), (size_t)max_frames);
  
  for (size_t submap_id = 0; submap_id < process_frames; ++submap_id) {
    std::cout << "\n=== Processing Frame " << submap_id << " ===" << std::endl;
    
    pcl::PointCloud<pcl::PointXYZI>::Ptr cloud(
        new pcl::PointCloud<pcl::PointXYZI>());
    pcl::PointCloud<pcl::PointXYZI> transform_cloud;
    cloud->reserve(1000000);
    transform_cloud.reserve(1000000);
    
    // STAGE 1: Load point cloud
    Eigen::Vector3d translation = pose_list[submap_id].first;
    Eigen::Matrix3d rotation = pose_list[submap_id].second;
    
    if (read_bin) {
      std::stringstream ss;
      ss << pcds_dir << "/" << std::setfill('0') << std::setw(6) << submap_id << ".bin";
      std::string pcd_file = ss.str();
      
      auto t_load_start = std::chrono::high_resolution_clock::now();
      std::vector<float> lidar_data = read_lidar_data(ss.str());
      if (lidar_data.size() == 0) {
        break;
      }

      for (std::size_t i = 0; i < lidar_data.size(); i += 4) {
        pcl::PointXYZI point;
        point.x = lidar_data[i];
        point.y = lidar_data[i + 1];
        point.z = lidar_data[i + 2];
        point.intensity = lidar_data[i + 3];
        cloud->points.push_back(point);
        
        Eigen::Vector3d pv = point2vec(point);
        pv = rotation * pv + translation;
        transform_cloud.points.push_back(vec2point(pv));
      }
      auto t_load_end = std::chrono::high_resolution_clock::now();
      std::cout << "[Time] load cloud: " << time_inc(t_load_end, t_load_start) << "ms" << std::endl;
    } else {
      // Load point cloud from pcd file
      std::stringstream ss;
      ss << pcds_dir << "/" << std::setfill('0') << std::setw(6) << submap_id << ".pcd";
      std::string pcd_file = ss.str();

      auto t_load_start = std::chrono::high_resolution_clock::now();
      if (reader.read(pcd_file, *cloud) == -1) {
        ROS_ERROR_STREAM("Couldn't read file " << pcd_file);
        continue;
      }
      auto t_load_end = std::chrono::high_resolution_clock::now();
      std::cout << "[Time] load cloud: " << time_inc(t_load_end, t_load_start) << "ms" << std::endl;
      
      transform_cloud = *cloud;
      for (size_t j = 0; j < transform_cloud.size(); j++) {
        Eigen::Vector3d pv = point2vec(transform_cloud.points[j]);
        pv = rotation * pv + translation;
        transform_cloud.points[j] = vec2point(pv);
      }
    }

    std::cout << "Loaded " << cloud->size() << " points" << std::endl;
    
    // Save Stage 1: Raw and transformed point clouds
    std::map<std::string, double> stage1_stats;
    stage1_stats["frame_id"] = (double)submap_id;
    stage1_stats["points_count"] = (double)cloud->size();
    stage1_stats["min_x"] = std::numeric_limits<double>::max();
    stage1_stats["max_x"] = std::numeric_limits<double>::lowest();
    stage1_stats["min_y"] = std::numeric_limits<double>::max();
    stage1_stats["max_y"] = std::numeric_limits<double>::lowest();
    stage1_stats["min_z"] = std::numeric_limits<double>::max();
    stage1_stats["max_z"] = std::numeric_limits<double>::lowest();
    
    for (const auto &point : cloud->points) {
      stage1_stats["min_x"] = std::min(stage1_stats["min_x"], (double)point.x);
      stage1_stats["max_x"] = std::max(stage1_stats["max_x"], (double)point.x);
      stage1_stats["min_y"] = std::min(stage1_stats["min_y"], (double)point.y);
      stage1_stats["max_y"] = std::max(stage1_stats["max_y"], (double)point.y);
      stage1_stats["min_z"] = std::min(stage1_stats["min_z"], (double)point.z);
      stage1_stats["max_z"] = std::max(stage1_stats["max_z"], (double)point.z);
    }
    save_stage_statistics(submap_id, "1_raw_pointcloud", stage1_stats, output_dir);
    
    // Save Stage 2: Transformed point cloud
    std::map<std::string, double> stage2_stats;
    stage2_stats["frame_id"] = (double)submap_id;
    stage2_stats["translation_x"] = translation.x();
    stage2_stats["translation_y"] = translation.y();
    stage2_stats["translation_z"] = translation.z();
    stage2_stats["min_x_transformed"] = std::numeric_limits<double>::max();
    stage2_stats["max_x_transformed"] = std::numeric_limits<double>::lowest();
    stage2_stats["min_y_transformed"] = std::numeric_limits<double>::max();
    stage2_stats["max_y_transformed"] = std::numeric_limits<double>::lowest();
    stage2_stats["min_z_transformed"] = std::numeric_limits<double>::max();
    stage2_stats["max_z_transformed"] = std::numeric_limits<double>::lowest();
    
    for (const auto &point : transform_cloud.points) {
      stage2_stats["min_x_transformed"] = std::min(stage2_stats["min_x_transformed"], (double)point.x);
      stage2_stats["max_x_transformed"] = std::max(stage2_stats["max_x_transformed"], (double)point.x);
      stage2_stats["min_y_transformed"] = std::min(stage2_stats["min_y_transformed"], (double)point.y);
      stage2_stats["max_y_transformed"] = std::max(stage2_stats["max_y_transformed"], (double)point.y);
      stage2_stats["min_z_transformed"] = std::min(stage2_stats["min_z_transformed"], (double)point.z);
      stage2_stats["max_z_transformed"] = std::max(stage2_stats["max_z_transformed"], (double)point.z);
    }
    save_stage_statistics(submap_id, "2_transformed_pointcloud", stage2_stats, output_dir);

    // STAGE 3: Descriptor Extraction
    std::cout << "[Description] submap id:" << submap_id << std::endl;
    auto t_descriptor_begin = std::chrono::high_resolution_clock::now();
    std::vector<BTC> btcs_vec;
    btc_manager->GenerateBtcDescs(transform_cloud.makeShared(), submap_id, btcs_vec);
    auto t_descriptor_end = std::chrono::high_resolution_clock::now();
    descriptor_time.push_back(time_inc(t_descriptor_end, t_descriptor_begin));
    
    // Save Stage 3a: Voxels and planes (extract from manager's internal data)
    std::map<std::string, double> stage3a_stats;
    stage3a_stats["frame_id"] = (double)submap_id;
    stage3a_stats["plane_count"] = (double)btc_manager->plane_cloud_vec_.back()->size();
    save_stage_statistics(submap_id, "3a_voxels_and_planes", stage3a_stats, output_dir);
    
    // Save Stage 3b: Binary descriptors
    std::map<std::string, double> stage3b_stats;
    stage3b_stats["frame_id"] = (double)submap_id;
    stage3b_stats["binary_descriptors_count"] = (double)btc_manager->history_binary_list_.back().size();
    
    if (!btc_manager->history_binary_list_.back().empty()) {
      int min_summary = btc_manager->history_binary_list_.back()[0].summary_;
      int max_summary = btc_manager->history_binary_list_.back()[0].summary_;
      double sum_summary = 0;
      
      for (const auto &bd : btc_manager->history_binary_list_.back()) {
        min_summary = std::min(min_summary, (int)bd.summary_);
        max_summary = std::max(max_summary, (int)bd.summary_);
        sum_summary += bd.summary_;
      }
      
      stage3b_stats["min_summary"] = (double)min_summary;
      stage3b_stats["max_summary"] = (double)max_summary;
      stage3b_stats["mean_summary"] = sum_summary / btc_manager->history_binary_list_.back().size();
    }
    save_stage_statistics(submap_id, "3b_binary_descriptors", stage3b_stats, output_dir);
    
    // Save Stage 3c: BTC descriptors
    std::map<std::string, double> stage3c_stats;
    stage3c_stats["frame_id"] = (double)submap_id;
    stage3c_stats["btc_count"] = (double)btcs_vec.size();
    
    if (!btcs_vec.empty()) {
      double min_side = btcs_vec[0].triangle_.minCoeff();
      double max_side = btcs_vec[0].triangle_.maxCoeff();
      double sum_side = 0;
      int count_sides = 0;
      
      for (const auto &btc : btcs_vec) {
        min_side = std::min(min_side, btc.triangle_.minCoeff());
        max_side = std::max(max_side, btc.triangle_.maxCoeff());
        sum_side += btc.triangle_.sum();
        count_sides += 3;
      }
      
      stage3c_stats["min_side_length"] = min_side;
      stage3c_stats["max_side_length"] = max_side;
      stage3c_stats["mean_side_length"] = sum_side / count_sides;
    }
    save_stage_statistics(submap_id, "3c_btc_descriptors", stage3c_stats, output_dir);
    
    std::cout << "Generated " << btcs_vec.size() << " BTC descriptors" << std::endl;
    
    // STAGE 4: Searching Loop
    auto t_query_begin = std::chrono::high_resolution_clock::now();
    std::pair<int, double> search_result(-1, 0);
    std::pair<Eigen::Vector3d, Eigen::Matrix3d> loop_transform;
    loop_transform.first << 0, 0, 0;
    loop_transform.second = Eigen::Matrix3d::Identity();
    std::vector<std::pair<BTC, BTC>> loop_std_pair;
    
    if (submap_id > config_setting.skip_near_num_) {
      if (btcs_vec.size() == 0) {
        // No BTCs case handled
      } else {
        btc_manager->SearchLoop(btcs_vec, search_result, loop_transform, loop_std_pair);
      }
    }
    
    // Save Stage 4: Loop detection
    std::map<std::string, double> stage4_stats;
    stage4_stats["frame_id"] = (double)submap_id;
    stage4_stats["loop_detected"] = search_result.first >= 0 ? 1.0 : 0.0;
    stage4_stats["loop_id"] = (double)search_result.first;
    stage4_stats["loop_score"] = search_result.second;
    stage4_stats["matches_count"] = (double)loop_std_pair.size();
    stage4_stats["database_size"] = (double)btc_manager->data_base_.size();
    
    if (search_result.first > 0) {
      std::cout << "[Loop Detection] trigger loop: " << submap_id << "--"
                << search_result.first << ", score:" << search_result.second << std::endl;
      triggle_loop_num++;
      trigger_loop_list.push_back(std::make_tuple(submap_id, search_result.first, search_result.second));
      
      // Ground truth verification
      down_sampling_voxel(transform_cloud, 0.5);
      btc_manager->key_cloud_vec_.push_back(transform_cloud.makeShared());
      
      double cloud_overlap = calc_overlap(transform_cloud.makeShared(),
                                        btc_manager->key_cloud_vec_[search_result.first], 0.5);
      stage4_stats["ground_truth_overlap"] = cloud_overlap;
      
      if (cloud_overlap >= cloud_overlap_thr) {
        true_loop_num++;
        stage4_stats["is_true_positive"] = 1.0;
        true_loop_list.push_back(std::make_tuple(submap_id, search_result.first, cloud_overlap));
        std::cout << "TRUE POSITIVE: overlap = " << cloud_overlap << std::endl;
      } else {
        stage4_stats["is_true_positive"] = 0.0;
        false_loop_list.push_back(std::make_tuple(submap_id, search_result.first, cloud_overlap));
        std::cout << "FALSE POSITIVE: overlap = " << cloud_overlap << std::endl;
      }
    } else {
      if (submap_id <= config_setting.skip_near_num_) {
        stage4_stats["skip_reason"] = 1.0; // too_early
      } else {
        stage4_stats["skip_reason"] = 2.0; // no_btc_descriptors
      }
    }
    
    save_stage_statistics(submap_id, "4_loop_detection", stage4_stats, output_dir);
    
    auto t_query_end = std::chrono::high_resolution_clock::now();
    querying_time.push_back(time_inc(t_query_end, t_query_begin));

    // STAGE 5: Add descriptors to the database
    auto t_map_update_begin = std::chrono::high_resolution_clock::now();
    btc_manager->AddBtcDescs(btcs_vec);
    auto t_map_update_end = std::chrono::high_resolution_clock::now();
    update_time.push_back(time_inc(t_map_update_end, t_map_update_begin));
    
    // Save Stage 5: Database update
    std::map<std::string, double> stage5_stats;
    stage5_stats["frame_id"] = (double)submap_id;
    stage5_stats["database_entries"] = (double)btc_manager->data_base_.size();
    stage5_stats["key_clouds_count"] = (double)btc_manager->key_cloud_vec_.size();
    
    // Only save downsampled cloud if we haven't already (to avoid duplicate)
    if (search_result.first <= 0) {
      down_sampling_voxel(transform_cloud, 0.5);
      btc_manager->key_cloud_vec_.push_back(transform_cloud.makeShared());
    }
    stage5_stats["downsampled_cloud_size"] = (double)transform_cloud.size();
    
    save_stage_statistics(submap_id, "5_database_update", stage5_stats, output_dir);
    
    std::cout << "[Time] descriptor extraction: "
              << time_inc(t_descriptor_end, t_descriptor_begin) << "ms, "
              << "query: " << time_inc(t_query_end, t_query_begin) << "ms, "
              << "update map:"
              << time_inc(t_map_update_end, t_map_update_begin) << "ms"
              << std::endl;
    std::cout << std::endl;

    // Save detailed results for key frames
    if (submap_id % 10 == 0 || (search_result.first >= 0)) {
      // Save detailed binary descriptors
      std::stringstream binary_ss;
      binary_ss << output_dir << "/frame_" << std::setfill('0') << std::setw(6) 
                << submap_id << "_binary_detailed.txt";
      save_binary_descriptors_txt(btc_manager->history_binary_list_.back(), binary_ss.str());
      
      // Save detailed BTC descriptors
      std::stringstream btc_ss;
      btc_ss << output_dir << "/frame_" << std::setfill('0') << std::setw(6) 
             << submap_id << "_btc_detailed.txt";
      save_btc_descriptors_txt(btcs_vec, btc_ss.str());
      
      // Save detailed planes
      std::stringstream planes_ss;
      planes_ss << output_dir << "/frame_" << std::setfill('0') << std::setw(6) 
                << submap_id << "_planes_detailed.txt";
      save_planes_txt(*btc_manager->plane_cloud_vec_.back(), planes_ss.str());
    }

    // Visualization (kept for compatibility)
    sensor_msgs::PointCloud2 pub_cloud;
    pcl::toROSMsg(transform_cloud, pub_cloud);
    pub_cloud.header.frame_id = "camera_init";
    pubCureentCloud.publish(pub_cloud);

    pcl::PointCloud<pcl::PointXYZ> key_points_cloud;
    for (auto var : btc_manager->history_binary_list_.back()) {
      pcl::PointXYZ pi;
      pi.x = var.location_[0];
      pi.y = var.location_[1];
      pi.z = var.location_[2];
      key_points_cloud.push_back(pi);
    }
    pcl::toROSMsg(key_points_cloud, pub_cloud);
    pub_cloud.header.frame_id = "camera_init";
    pubCurrentBinary.publish(pub_cloud);

    // Progress indicator
    if ((submap_id + 1) % 10 == 0) {
      std::cout << "\nProgress: " << submap_id + 1 << "/" << process_frames 
                << " frames processed" << std::endl;
      std::cout << "Loops detected so far: " << triggle_loop_num << std::endl;
    }

    loop.sleep();
  }

  // STAGE 6: Save final statistics
  double mean_descriptor_time =
      std::accumulate(descriptor_time.begin(), descriptor_time.end(), 0.0) / descriptor_time.size();
  double mean_query_time =
      std::accumulate(querying_time.begin(), querying_time.end(), 0.0) / querying_time.size();
  double mean_update_time =
      std::accumulate(update_time.begin(), update_time.end(), 0.0) / update_time.size();
      
  std::cout << "Total submap number:" << process_frames
            << ", trigger loop number:" << triggle_loop_num
            << ", true loop number:" << true_loop_num << std::endl;
  std::cout << "Mean time for descriptor extraction: " << mean_descriptor_time
            << "ms, query: " << mean_query_time
            << "ms, update: " << mean_update_time << "ms, total: "
            << mean_descriptor_time + mean_query_time + mean_update_time << "ms"
            << std::endl;
            
  // Save comprehensive final statistics
  std::string final_stats_file = output_dir + "/final_statistics.json";
  std::ofstream final_ofs(final_stats_file);
  
  final_ofs << "{" << std::endl;
  final_ofs << "  \"processed_frames\": " << process_frames << "," << std::endl;
  final_ofs << "  \"total_loops\": " << triggle_loop_num << "," << std::endl;
  final_ofs << "  \"true_loops\": " << true_loop_num << "," << std::endl;
  final_ofs << "  \"false_loops\": " << (triggle_loop_num - true_loop_num) << "," << std::endl;
  final_ofs << "  \"precision\": " << (triggle_loop_num > 0 ? (double)true_loop_num / triggle_loop_num : 0.0) << "," << std::endl;
  final_ofs << "  \"timing\": {" << std::endl;
  final_ofs << "    \"mean_descriptor_time\": " << mean_descriptor_time << "," << std::endl;
  final_ofs << "    \"mean_query_time\": " << mean_query_time << "," << std::endl;
  final_ofs << "    \"mean_update_time\": " << mean_update_time << "," << std::endl;
  final_ofs << "    \"std_descriptor_time\": ";
  
  // Calculate standard deviations
  double sum_sq_desc = 0, sum_sq_query = 0, sum_sq_update = 0;
  for (size_t i = 0; i < descriptor_time.size(); i++) {
    sum_sq_desc += (descriptor_time[i] - mean_descriptor_time) * (descriptor_time[i] - mean_descriptor_time);
    sum_sq_query += (querying_time[i] - mean_query_time) * (querying_time[i] - mean_query_time);
    sum_sq_update += (update_time[i] - mean_update_time) * (update_time[i] - mean_update_time);
  }
  
  double std_desc = sqrt(sum_sq_desc / descriptor_time.size());
  double std_query = sqrt(sum_sq_query / querying_time.size());
  double std_update = sqrt(sum_sq_update / update_time.size());
  
  final_ofs << std_desc << "," << std::endl;
  final_ofs << "    \"std_query_time\": " << std_query << "," << std::endl;
  final_ofs << "    \"std_update_time\": " << std_update << std::endl;
  final_ofs << "  }," << std::endl;
  
  // Save loop details
  final_ofs << "  \"loop_details\": [" << std::endl;
  for (size_t i = 0; i < std::min(trigger_loop_list.size(), (size_t)10); i++) {
    bool is_true_positive = false;
    for (const auto &true_loop : true_loop_list) {
      if (std::get<0>(true_loop) == std::get<0>(trigger_loop_list[i])) {
        is_true_positive = true;
        break;
      }
    }
    
    final_ofs << "    {" << std::endl;
    final_ofs << "      \"current_frame\": " << std::get<0>(trigger_loop_list[i]) << "," << std::endl;
    final_ofs << "      \"matched_frame\": " << std::get<1>(trigger_loop_list[i]) << "," << std::endl;
    final_ofs << "      \"score\": " << std::get<2>(trigger_loop_list[i]) << "," << std::endl;
    final_ofs << "      \"status\": \"" << (is_true_positive ? "TP" : "FP") << "\"" << std::endl;
    final_ofs << "    }";
    if (i < std::min(trigger_loop_list.size(), (size_t)10) - 1) final_ofs << ",";
    final_ofs << std::endl;
  }
  final_ofs << "  ]," << std::endl;
  
  final_ofs << "  \"database_final_size\": " << btc_manager->data_base_.size() << std::endl;
  final_ofs << "}" << std::endl;
  
  final_ofs.close();
  
  // Save detailed timing data
  std::string timing_file = output_dir + "/timing_data.txt";
  std::ofstream timing_ofs(timing_file);
  timing_ofs << "# frame_id descriptor_time query_time update_time" << std::endl;
  for (size_t i = 0; i < descriptor_time.size(); i++) {
    timing_ofs << i << " " << descriptor_time[i] << " " << querying_time[i] << " " << update_time[i] << std::endl;
  }
  timing_ofs.close();
  
  // Save loop detection results
  std::string loops_file = output_dir + "/loop_results.txt";
  std::ofstream loops_ofs(loops_file);
  loops_ofs << "# Detected loops: current_frame matched_frame score status overlap" << std::endl;
  for (const auto &trigger_loop : trigger_loop_list) {
    bool is_true_positive = false;
    double overlap = 0.0;
    
    for (const auto &true_loop : true_loop_list) {
      if (std::get<0>(true_loop) == std::get<0>(trigger_loop)) {
        is_true_positive = true;
        overlap = std::get<2>(true_loop);
        break;
      }
    }
    
    if (!is_true_positive) {
      for (const auto &false_loop : false_loop_list) {
        if (std::get<0>(false_loop) == std::get<0>(trigger_loop)) {
          overlap = std::get<2>(false_loop);
          break;
        }
      }
    }
    
    loops_ofs << std::get<0>(trigger_loop) << " " << std::get<1>(trigger_loop) << " " 
              << std::get<2>(trigger_loop) << " " << (is_true_positive ? "TP" : "FP") 
              << " " << overlap << std::endl;
  }
  loops_ofs.close();
  
  std::cout << "\nVerification complete! Results saved in: " << output_dir << std::endl;
  
  delete btc_manager;
  return 0;
}
