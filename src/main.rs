extern crate bincode;
extern crate tokio;
extern crate redis;
pub mod game;
pub mod server_net;

use server_net::message::*;
use server_net::kafka_client::*;
use std::mem;
use game::room::GameRoomMng;
use tokio::sync::mpsc::{ channel, Sender };
use redis::AsyncCommands;
use futures::*;
use chrono::prelude::*;
use rdkafka::producer::{ FutureProducer };
use rdkafka::config::{ ClientConfig };

use game::*;

async fn send_data(sender: &mut Sender<Vec<u8>>, user_id: &i64, data: Vec<u8>) {
    let user_id = user_id.to_le_bytes();
    let user_id: Vec<u8> = user_id.iter().cloned().collect();
    let data = [user_id.clone(), data].concat();
    match sender.send(data).await {
        Ok(()) => {},
        Err(_) => {
            // TODO
        }
    }
}

fn parse_authorized_user_id(msg: &Vec<u8>) -> i64 {
    let buf: &[u8] = &msg;
    let mut authorized_buf = [0u8; AUTHORIZED_INFO_SIZE];
    for i in 0..AUTHORIZED_INFO_SIZE {
        authorized_buf[i] = buf[i];
    }
    let authorized_user_id : i64 = i64::from_le_bytes(authorized_buf);
    return authorized_user_id;
}

#[tokio::main]
async fn main() {
    let (req_tx, mut req_rx)= channel::<Vec<u8>>(4096);
    let (mut rsp_tx, mut rsp_rx)= channel::<Vec<u8>>(4096);
    let req_tx_copy = req_tx.clone();
    let redis_addr = "redis://127.0.0.1:6379/".to_string();
    let redis_uri = redis_addr.clone();
    let broker_addr = "127.0.0.1:9092".to_string();
    let worker_id: i64 = 1;
    let max_open_room = 5;
    let topic = format!("majiang_room_{}_r1p1", worker_id);
    let rsp_topic = format!("majiang_room_{}_r1p1_rsp", worker_id);
    let group_id = format!("majiang_room_group:{}:", worker_id);
    let topic_list_key = "topiclist:";
    let redis_client = redis::Client::open(redis_uri.clone()).unwrap();
    let mut conn = redis_client.get_async_connection().await.unwrap();
    match conn.zrank(topic_list_key.clone(), topic.clone()).await {
        Ok(v) => {
            match redis::from_redis_value::<i64>(&v) {
                Ok(rank) => {},
                Err(_) => {
                    println!("no this topic:{}, create it", topic);
                    let _: i64 = match conn.zadd(topic_list_key.clone(), topic.clone(), -1).await {
                        Ok(v) => {
                            v
                        },
                        Err(err) => {
                            println!("redis add worker_id:{}, topic:{} fail err: {}", worker_id, topic, err);
                            0
                        }
                    };
                }
            }
        }
        Err(err) => {
            println!("redis query worker_id:{}, topic:{} fail err: {}", worker_id, topic, err);
            return;
        }
    };

    let broker_addr_copy = broker_addr.clone();
    tokio::spawn(async move {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &broker_addr_copy)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        loop {
            let msg = rsp_rx.recv().await.unwrap();
            produce(&producer, &rsp_topic, &msg).await;
        }
    });

    let topic_copy = topic.clone();
    tokio::spawn(async move {
        let mut room_mng = GameRoomMng::new(max_open_room, redis_addr, topic_copy);
        loop {
            let msg = req_rx.recv().await.unwrap();
            let authorized_user_id = parse_authorized_user_id(&msg);
            let buf: &[u8] = &msg;
            let header = bincode::deserialize::<Header> (&buf[AUTHORIZED_INFO_SIZE..AUTHORIZED_INFO_SIZE + HEADER_SIZE]).unwrap();
            println!("recv msg_type:{} from user:{}", header.msg_type, authorized_user_id);
            match unsafe { mem::transmute(header.msg_type) } {
                MsgType::GameOp => {
                    match bincode::deserialize::<GameOperation> (&buf[AUTHORIZED_INFO_SIZE..]) {
                        Ok(game_op) => {
                            unsafe {
                                let room_id: Vec<u8> = game_op.game_info.room_id.iter().cloned().collect();
                                let room_id = String::from_utf8(room_id).unwrap();
                                if let Some(mut sender) = room_mng.get_room_notifier(&room_id) {
                                    println!("recv provide:{:?} target:{}", game_op.provide_cards, game_op.target);
                                    sender.send(msg).await;
                                }
                            }
                        },
                        Err(err) => {
                            println!("parse message err: {:?}", err);
                        }
                    }
                },
                MsgType::RoomOp => {
                    let op = bincode::deserialize::<RoomManage> (&buf[AUTHORIZED_INFO_SIZE..]).unwrap();
                    let mut msg = RoomManageResult {
                        header: Header::new(MsgType::RoomManageResult),
                        op_type: op.op_type,
                        user_id: op.user_id,
                        code: 0,
                        room_id: vec![0; 6],
                    };
                    let room_id: Vec<u8> = op.room_id.iter().cloned().collect();
                    let mut room_id = String::from_utf8(room_id).unwrap();
                    match unsafe { mem::transmute(op.op_type) } {
                        OpType::CreateRoom => {
                            let (created_room_id, code) = room_mng.create_room(op.user_id).await;
                            msg.room_id = created_room_id.clone().into_bytes();
                            msg.code = unsafe { mem::transmute(code) };
                            room_id = created_room_id;
                            unsafe { println!("user:{} create room:{}", op.user_id.clone(), room_id) };
                        },
                        OpType::JoinRoom => {
                            let (err, code) = room_mng.join_room(op.user_id, &room_id).await;
                            msg.room_id = err.into_bytes();
                            msg.code = unsafe { mem::transmute(code) };
                        },
                        OpType::LeaveRoom => {
                            let (err, code) = room_mng.leave_room(op.user_id, &room_id).await;
                            msg.room_id = err.into_bytes();
                            msg.code = unsafe { mem::transmute(code) };
                        },
                        OpType::ReadyRoom => {
                            let (err, code) = room_mng.ready_room(op.user_id, &room_id);
                            msg.room_id = err.into_bytes();
                            msg.code = unsafe { mem::transmute(code) };
                        },
                        OpType::CancelReady => {
                            let (err, code) = room_mng.cancel_ready(op.user_id, &room_id);
                            msg.room_id = err.into_bytes();
                            msg.code = unsafe { mem::transmute(code) };
                        },
                        _ => {}
                    }
                    msg.header.len = msg.size() as i32;
                    let data: Vec<u8> = bincode::serialize::<RoomManageResult>(&msg).unwrap();
                    println!("data len:{}", data.len());
                    send_data(&mut rsp_tx, &authorized_user_id, data).await;
                    room_mng.show_room_state();

                    if let Some(room_users) = room_mng.get_room_user_id(&room_id) {
                        let snapshot = room_mng.get_room_snapshot(&room_id).unwrap();
                        let data: Vec<u8> = bincode::serialize::<RoomSnapshot>(&snapshot).unwrap();
                        for user_id in room_users.iter() {
                            if *user_id == -1 {
                                continue;
                            }
                            send_data(&mut rsp_tx, &user_id, data.clone()).await;
                        }
                        if let Some(all_ready) = room_mng.all_ready(&room_id) {
                            if all_ready  && !room_mng.room_has_start(&room_id) {
                                let mut update = RoomUpdate {
                                    header: Header::new(MsgType::RoomUpdate),
                                    op_type: unsafe { mem::transmute(OpType::StartRoom) },
                                    user_id: 0,
                                    room_id: room_id.clone().into_bytes(),
                                };
                                update.header.len = update.size() as i32;
                                let data: Vec<u8> = bincode::serialize::<RoomUpdate>(&update).unwrap();
                                for user_id in room_users.iter() {
                                    send_data(&mut rsp_tx, &user_id, data.clone()).await;
                                }
                                let (game_msg_tx, game_msg_rx) = channel::<Vec<u8>>(4096);
                                room_mng.set_room_notifier(&room_id, game_msg_tx);
                                tokio::spawn(start_game(room_id.clone(), room_users.clone(), rsp_tx.clone(), game_msg_rx, req_tx_copy.clone()));
                            }
                        }
                    }
                },
                MsgType::GameOver => {
                    let over = bincode::deserialize::<GameOver> (&buf[AUTHORIZED_INFO_SIZE..]).unwrap();
                    let mut room_id = String::from_utf8(over.room_id.clone()).unwrap();
                    room_mng.room_game_over(&room_id);
                    if let Some(room_users) = room_mng.get_room_user_id(&room_id) {
                        let snapshot = room_mng.get_room_snapshot(&room_id).unwrap();
                        let data: Vec<u8> = bincode::serialize::<RoomSnapshot>(&snapshot).unwrap();
                        println!("room:{} over, broadcast room snapshot", room_id);
                        for user_id in room_users.iter() {
                            send_data(&mut rsp_tx, &user_id, data.clone()).await;
                        }
                    }
                },
                MsgType::Authen => {
                    if let Some(room_id) = room_mng.get_user_room_id(authorized_user_id) {
                        if let Some(mut sender) = room_mng.get_room_notifier(&room_id) {
                            let mut query = QueryGameSnapshot {
                                header: Header::new(MsgType::QueryGameState),
                                user_id: authorized_user_id,
                            };
                            query.header.len = query.size() as i32;
                            let data: Vec<u8> = bincode::serialize::<QueryGameSnapshot>(&query).unwrap();
                            send_data(&mut sender, &authorized_user_id, data).await;
                        } else {
                            let snapshot = room_mng.get_room_snapshot(&room_id).unwrap();
                            let data: Vec<u8> = bincode::serialize::<RoomSnapshot>(&snapshot).unwrap();
                            send_data(&mut rsp_tx, &authorized_user_id, data).await;
                        }
                    }
                }
                _ => {}
            }
        }
    });

    let topics = [topic.as_str()];
    consume(&broker_addr, &group_id, &topics, req_tx).await;
}
