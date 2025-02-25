@Configuration
@EnableRetry
public class RetryAdviceConfig implements BeanPostProcessor, Ordered {

    private static final Logger LOG = LoggerFactory.getLogger(RetryAdviceConfig.class);

    private static MethodInterceptor COCKROACH_RETRY_INTERCEPTOR_INSTANCE;
    private final String RETRY_SQL_STATE = "40001";

    @Value("${crdb.max-retry-attempts:3}")
    private int maxRetryAttempts;

    @Override
    public int getOrder() {
        return Integer.MAX_VALUE;
    }

    @Override
    @Nullable
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof AggregatorAppDetailsActivationService || bean instanceof ConsentClientAppRegistrationService || bean instanceof ConsentClientAppModifyService) {
            Advised advised = (Advised) bean;
            advised.addAdvice(0, getCockroachRetryInteceptor(maxRetryAttempts));
        }
        return bean;
    }

    /**
     * catches the conditional thrown via a jpa transactional exception.
     * org.springframework.orm.jpa.JpaSystemException: Unable to commit against JDBC Connection...
     * Caused by: org.hibernate.TransactionException: Unable to commit against JDBC Connection...
     * Caused by: org.postgresql.util.PSQLException: ERROR: restart transaction...
     */
    @Bean("cockroach-retry-interceptor")
    public MethodInterceptor getCockroachRetryInteceptor(@Value("${crdb.max-retry-attempts}")int maxRetryAttempts) {
        if (RetryAdviceConfig.COCKROACH_RETRY_INTERCEPTOR_INSTANCE == null) {
            SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(maxRetryAttempts);
            FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
            fixedBackOffPolicy.setBackOffPeriod(1000L);

            ExceptionClassifierRetryPolicy exceptionRetryPolicy = new ExceptionClassifierRetryPolicy();
            exceptionRetryPolicy.setExceptionClassifier(new Classifier<Throwable, RetryPolicy>() {
                @Override
                public RetryPolicy classify(Throwable classifiable) {
                    String sqlState = null;
                    if (classifiable instanceof SQLException exception) {
                        sqlState = exception.getSQLState();
                    } else if (classifiable instanceof JDBCException exception) {
                        sqlState = exception.getSQLState();
                    } else if (classifiable.getCause() instanceof JDBCException) {
                        JDBCException jdbcEx = (JDBCException) classifiable.getCause();
                        sqlState = jdbcEx.getSQLState();
                    } else if (classifiable.getCause() instanceof TransactionException) {
                        TransactionException trnsEx = (TransactionException) classifiable.getCause();
                        if (trnsEx.getCause() instanceof SQLException) {
                            SQLException psqlEx = (SQLException) trnsEx.getCause();
                            sqlState = psqlEx.getSQLState();
                        }
                    }
                    if (shouldDoRetry(classifiable.getClass().toString(), sqlState)) {
                        return simpleRetryPolicy;
                    }
                    LOG.error("Exception of {} with cause {} not retryable :: message={}", classifiable.getClass(), classifiable.getCause(), classifiable.getMessage());
                    LOG.error("Exception cause", classifiable);
                    return new NeverRetryPolicy();
                }
            });
            MethodInterceptor interceptor = RetryInterceptorBuilder
                    .stateless()
                    .label("cockroach-retry-interceptor")
                    .retryPolicy(exceptionRetryPolicy)
                    .backOffPolicy(fixedBackOffPolicy)
                    .build();
            RetryAdviceConfig.COCKROACH_RETRY_INTERCEPTOR_INSTANCE = interceptor;
        }
        return RetryAdviceConfig.COCKROACH_RETRY_INTERCEPTOR_INSTANCE;
    }

    boolean shouldDoRetry(String clazz, String sqlState) {
        if (sqlState != null && RETRY_SQL_STATE.equals(sqlState)){
            LOG.info("{} :: Retrying database operation SqlState={}", clazz, sqlState);
            return true;
        } else {
            LOG.info("{} with SqlState={} not retryable", clazz, sqlState);
            return false;
        }
    }
}
