# GigaMaps

## Global school connectivity map

An open & live global map of schools and their connectivity.

[**What is Giga Maps?**](#what-is-giga-maps) |
[**What is Giga?**](#what-is-giga)

## What is Giga Maps?

Giga Maps aims to build a global, dynamic map that showcases the status of school connectivity in the world. This tool helps to identify gaps in school connectivity, create transparency, estimate the capital investment needed to connect unconnected schools, develop plans, attract the necessary funding, and track progress towards achieving universal school Internet connectivity.

## What is Giga?

Giga is a UNICEF-ITU global initiative to connect every school to the Internet and every young person to information, opportunity, and choice.

## School Location
### Locate schools through our open map
2.1M schools location collected from 50 government bodies, Open data sources like OSM and Giga's AI model.

## School Connectivity
### Monitor real-time connectivity of schools
Real time connectivity monitored through GigaCheck Desktop App and chrome extension, Brazil's nic.br app and multiple ISP collaborations.

## Infrastructure
### Understand connectivity infrastructure available around schools
Infrastructure data contributed by ~10 partners like ITU, Meta, GSMA, and government bodies.

## Tasks

| Sr <br/>No | Task Name                                                         | Path                                                                                       | Description                                                                             | Frequency                                     |
|------------|-------------------------------------------------------------------|--------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|-----------------------------------------------|
| 1.         | update_all_cached_values                                          | proco.utils.tasks.update_all_cached_values                                                 | Update cached values in Backend Redis                                                   | 4:00 AM Every day                             |
| 2.         | rebuild_school_index                                              | proco.utils.tasks.rebuild_school_index                                                     | Create the Cognitive Search Index                                                       | 2:00 AM Every day                             |
| 3.         | clean_old_realtime_data                                           | proco.connection_statistics.tasks.clean_old_realtime_data                                  | Delete more than 30 days old data from RealTimeConnectivity table                       | 5:00 AM Every day                             |
| 4.         | update_school_records                                             | proco.schools.tasks.update_school_records                                                  | Populate the connectivity_status and coverage_status field in School table              | 1:00 AM, 1:00 PM Every day                    |
| 5.         | cleanup_school_master_rows                                        | proco.data_sources.tasks.cleanup_school_master_rows                                        | Cleanup School Master data                                                              | 1:40 AM, 3:40 PM Every day                    |
| 6.         | update_static_data                                                | proco.data_sources.tasks.update_static_data                                                | Fetch the data from School Master data source                                           | 3:00 AM Every day                             |
| 7.         | update_live_data                                                  | proco.data_sources.tasks.update_live_data                                                  | Fetch the live data from PCDC and QoS data source                                       | 2:10 AM, 8:10 AM, 2:10 PM , 8:10 PM Every day |
| 8.         | update_live_data_and_aggregate_yesterday_data                     | proco.data_sources.tasks.update_live_data                                                  | Fetch the latest live data and aggregate the previous daya data                         | 00:30 AM every day                            |
| 9.         | populate_school_registration_data                                 | proco.utils.tasks.populate_school_registration_data                                        | Populate the RT status for newly added schools in Live/Static data source               | 2:40 AM Every day                             |                                              |
| 10.        | handle_published_school_master_data_row                           | proco.data_sources.tasks.handle_published_school_master_data_row                           | Handle Published School Master data records                                             | Every 4 hour at 10th minute                   |
| 11.        | handle_deleted_school_master_data_row                             | proco.data_sources.tasks.handle_deleted_school_master_data_row                             | Delete the School from Proco DB if deleted by School Master data source                 | 00:10 AM every day                            |
| 12.        | email_reminder_to_editor_and_publisher_for_review_waiting_records | proco.data_sources.tasks.email_reminder_to_editor_and_publisher_for_review_waiting_records | Send reminder mail to Editors and Publishers to review the School Master records if any | 08:10 AM every day                            |
| 13.        | clean_old_live_data                                               | proco.data_sources.tasks.clean_old_live_data                                               | Cleanup for QoS and PCDC source data table for data older than 30 days                  | 05:10 AM every day                            |

