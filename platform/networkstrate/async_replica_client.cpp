/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "platform/networkstrate/async_replica_client.h"
#include "platform/networkstrate/traffic.h"

#include <linux/sockios.h>
#include <sys/ioctl.h>

#include <boost/bind/bind.hpp>

#include "platform/common/queue/lock_free_queue.h"
#include "platform/proto/replica_info.pb.h"

namespace resdb {

namespace {
constexpr int64_t kMaxPendingBytes = 256LL << 20;
}  // namespace

AsyncReplicaClient::AsyncReplicaClient(boost::asio::io_service* io_service,
                                       const std::string& ip, int port,
                                       bool is_use_long_conn)
    : socket_(*io_service),
      retry_timer_(*io_service),
      endpoint_(boost::asio::ip::address::from_string(ip), port),
      in_process_(false) {}

AsyncReplicaClient::~AsyncReplicaClient() {}

int64_t AsyncReplicaClient::PendingBytes() {
  int64_t pending = pending_bytes_.load();
  int unsent = 0;
  if (socket_.is_open() && ioctl(socket_.native_handle(), SIOCOUTQ, &unsent) == 0) {
    pending += unsent;
  }
  return pending;
}

int AsyncReplicaClient::SendMessage(const std::string& data) {
  // A peer that has not accepted data for this long is unreachable: drop
  // rather than queue without bound.
  if (pending_bytes_.load() > kMaxPendingBytes) return -1;
  pending_bytes_ += static_cast<int64_t>(data.size());
  NetworkTraffic::TxBytes().fetch_add(static_cast<int64_t>(data.size()),
                                      std::memory_order_relaxed);
  queue_.Push(std::make_unique<std::string>(data));
  if (!in_process_.load()) {
    bool old_value = false;
    if (in_process_.compare_exchange_strong(old_value, true,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acq_rel)) {
      OnSendNewMessage();
    }
  }
  return 0;
}

void AsyncReplicaClient::OnSendNewMessage() {
  std::unique_ptr<std::string> data = queue_.Pop(0);
  while (data == nullptr || data->empty()) {
    in_process_ = false;
    // A message pushed after the Pop above, by a sender that still saw
    // in_process_ set, would otherwise wait for the next SendMessage.
    if (queue_.Empty()) return;
    bool old_value = false;
    if (!in_process_.compare_exchange_strong(old_value, true,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acq_rel)) {
      return;  // that sender started the send itself
    }
    data = queue_.Pop(0);
  }
  pending_data_ = std::move(data);
  status_ = 0;
  OnSendMessage();
}

void AsyncReplicaClient::OnSendMessage() {
  if (status_ == 0) {
    data_size_ = pending_data_->size();
    sending_data_size_ = sizeof(data_size_);
    sending_data_ptr_ = reinterpret_cast<char*>(&data_size_);
    sending_data_idx_ = 0;
    status_ = 1;
    OnSend();
  } else if (status_ == 1) {
    sending_data_size_ = data_size_;
    sending_data_ptr_ = pending_data_->c_str();
    sending_data_idx_ = 0;
    status_ = 2;
    OnSend();
  } else {
    status_ = 0;
    pending_bytes_ -= static_cast<int64_t>(pending_data_->size());
    OnSendNewMessage();
  }
}

void AsyncReplicaClient::OnSend() {
  socket_.async_write_some(
      boost::asio::buffer(sending_data_ptr_ + sending_data_idx_,
                          sending_data_size_ - sending_data_idx_),
      [&](const boost::system::error_code& error, size_t send_size) {
        if (error) {
          ReConnect();
        } else {
          sending_data_idx_ += send_size;
          if (sending_data_idx_ >= sending_data_size_) {
            OnSendMessage();
          } else {
            OnSend();
          }
        }
      });
}

void AsyncReplicaClient::ReConnect() {
  socket_.async_connect(endpoint_, [&](const boost::system::error_code& error) {
    if (!error) {
      status_ = 0;
      OnSendMessage();
    } else {
      retry_timer_.expires_from_now(std::chrono::milliseconds(10));
      retry_timer_.async_wait([&](const boost::system::error_code&) { ReConnect(); });
    }
  });
}

}  // namespace resdb
