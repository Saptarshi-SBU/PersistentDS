#include "boost_logger.h"

void init_boost_logger(void)
{
    /* init boost log
     * 1. Add common attributes
     * 2. set log filter to trace
     */
    boost::log::add_common_attributes();
    boost::log::core::get()->set_filter(
#if LOGLEVELINFO            
        boost::log::trivial::severity >= boost::log::trivial::info
#elif LOGLEVELDEBUG        
        boost::log::trivial::severity >= boost::log::trivial::debug
#else        
        boost::log::trivial::severity >= boost::log::trivial::trace
#endif        
    );

    /* log formatter:
     * [TimeStamp] [Severity Level] Log message
     */
    auto fmtTimeStamp = boost::log::expressions::
        format_date_time<boost::posix_time::ptime>("TimeStamp", "%Y-%m-%d %H:%M:%S.%f");
    auto fmtSeverity = boost::log::expressions::
        attr<boost::log::trivial::severity_level>("Severity");
    boost::log::formatter logFmt =
        boost::log::expressions::format("[%1%] [%2%] %3%")
        % fmtTimeStamp % fmtSeverity
        % boost::log::expressions::smessage;

    /* console sink */
    auto consoleSink = boost::log::add_console_log(std::clog);
    consoleSink->set_formatter(logFmt);

    /* fs sink */
    auto fsSink = boost::log::add_file_log(
        boost::log::keywords::file_name = "test_%Y-%m-%d_%H-%M-%S.%N.log",
        boost::log::keywords::rotation_size = 10 * 1024 * 1024,
        boost::log::keywords::min_free_space = 30 * 1024 * 1024,
        boost::log::keywords::open_mode = std::ios_base::app);
    fsSink->set_formatter(logFmt);
    fsSink->locked_backend()->auto_flush(true);
}

