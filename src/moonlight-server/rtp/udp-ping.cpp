#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <helpers/logger.hpp>
#include <helpers/utils.hpp>
#include <rtp/udp-ping.hpp>
#include <thread>

namespace rtp {

void UDP_Server::start_receive() {
  socket_->async_receive_from(boost::asio::buffer(recv_buffer_),
                              remote_endpoint_,
                              0,
                              boost::bind(&UDP_Server::handle_receive,
                                          this,
                                          boost::asio::placeholders::error,
                                          boost::asio::placeholders::bytes_transferred));
}

void UDP_Server::handle_receive(const boost::system::error_code &error, std::size_t bytes_transferred) {
  if (!error) {
    auto addr = remote_endpoint_.address();
    std::string client_ip;
    if (addr.is_v6() && addr.to_v6().is_v4_mapped()) {
      client_ip = boost::asio::ip::make_address_v4(boost::asio::ip::v4_mapped, addr.to_v6()).to_string();
    } else {
      client_ip = addr.to_string();
    }
    auto client_port = remote_endpoint_.port();

    logs::log(logs::trace, "[RTP] Received ping from {}:{} ({} bytes)", client_ip, client_port, bytes_transferred);

    if (bytes_transferred == 4) {
      callback({.client_ip = client_ip, .client_port = client_port, .payload = {}});
    } else if (bytes_transferred >= sizeof(moonlight::SS_PING)) {
      auto ping = (moonlight::SS_PING *)recv_buffer_.data();
      callback({.client_ip = client_ip, .client_port = client_port, .payload = ping->payload});
    }
  } else {
    logs::log(logs::warning, "[RTP] Error receiving ping: {}", error.message());
  }

  // Continue to receive more data
  start_receive();
}

static std::shared_ptr<udp::socket> create_udp_socket(boost::asio::io_context &io_context,
                                                       const std::string &bind_ip,
                                                       unsigned short port) {
  boost::system::error_code ec;
  auto addr = boost::asio::ip::make_address(bind_ip, ec);
  if (ec) {
    logs::log(logs::warning, "[RTP] Invalid WOLF_BIND_IP '{}', falling back to ::", bind_ip);
    addr = boost::asio::ip::address_v6::any();
  }

  auto sock = std::make_shared<udp::socket>(io_context);
  udp::endpoint endpoint(addr, port);
  sock->open(endpoint.protocol(), ec);
  if (ec) {
    logs::log(logs::warning, "[RTP] Failed to open socket for {}: {}", addr.to_string(), ec.message());
    sock->open(udp::v4());
    sock->bind(udp::endpoint(udp::v4(), port));
    return sock;
  }

  sock->set_option(boost::asio::socket_base::reuse_address{true}, ec);
  if (addr.is_v6()) {
    sock->set_option(boost::asio::ip::v6_only{false}, ec);
  }

  sock->bind(endpoint, ec);
  if (ec) {
    logs::log(logs::warning,
              "[RTP] Failed to bind to {}:{}: {}, falling back to IPv4",
              addr.to_string(),
              port,
              ec.message());
    boost::system::error_code close_ec;
    sock->close(close_ec);
    sock->open(udp::v4());
    sock->set_option(boost::asio::socket_base::reuse_address{true});
    sock->bind(udp::endpoint(udp::v4(), port));
  }

  return sock;
}

void start_rtp_ping(unsigned short video_port,
                    unsigned short audio_port,
                    std::shared_ptr<wolf::core::events::EventBusType> event_bus) {
  auto io_context = std::make_shared<boost::asio::io_context>();

  try {
    logs::log(logs::info, "[RTP] Starting RTP ping server on ports {} and {}", video_port, audio_port);
    auto bind_ip = utils::get_env("WOLF_BIND_IP", "::");
    auto video_socket = create_udp_socket(*io_context, bind_ip, video_port);
    auto audio_socket = create_udp_socket(*io_context, bind_ip, audio_port);

    std::thread([io_context, video_socket, audio_socket, event_bus]() {
      UDP_Server video_server(video_socket, [event_bus, video_socket](const RTPPingEvent &ping) {
        logs::log(logs::trace, "[RTP] video from {}:{}", ping.client_ip, ping.client_port);
        auto ev = wolf::core::events::RTPVideoPingEvent{.client_ip = ping.client_ip,
                                                        .client_port = ping.client_port,
                                                        .video_socket = video_socket,
                                                        .payload = ping.payload};
        event_bus->fire_event(immer::box<wolf::core::events::RTPVideoPingEvent>(ev));
      });

      UDP_Server audio_server(audio_socket, [event_bus, audio_socket](const RTPPingEvent &ping) {
        logs::log(logs::trace, "[RTP] audio from {}:{}", ping.client_ip, ping.client_port);
        auto ev = wolf::core::events::RTPAudioPingEvent{.client_ip = ping.client_ip,
                                                        .client_port = ping.client_port,
                                                        .audio_socket = audio_socket,
                                                        .payload = ping.payload};
        event_bus->fire_event(immer::box<wolf::core::events::RTPAudioPingEvent>(ev));
      });

      io_context->run();
      logs::log(logs::info, "[RTP] server stopped");
    }).detach();

  } catch (std::exception &e) {
    logs::log(logs::warning, "[RTP] Unable to start RTP server: {}", e.what());
  }
}

} // namespace rtp