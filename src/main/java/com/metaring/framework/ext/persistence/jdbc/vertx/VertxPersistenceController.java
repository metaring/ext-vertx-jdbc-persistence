package com.metaring.framework.ext.persistence.jdbc.vertx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.calcite.linq4j.Linq4j;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import com.metaring.framework.SysKB;
import com.metaring.framework.Tools;
import com.metaring.framework.ext.vertx.VertxUtilties;
import com.metaring.framework.functionality.FunctionalityTransactionController;
import com.metaring.framework.persistence.OperationResult;
import com.metaring.framework.persistence.PersistenceController;
import com.metaring.framework.type.DataRepresentation;
import com.metaring.framework.type.series.TextSeries;

public class VertxPersistenceController implements PersistenceController {

    private static final CompletableFuture<Void> END = CompletableFuture.completedFuture(null);

    private JDBCClient jdbcClient;
    private SQLConnection sqlConnection;
    private boolean inTransaction = false;

    @Override
    public final CompletableFuture<FunctionalityTransactionController> init(SysKB sysKB, Executor asyncExecutor) {
        final CompletableFuture<FunctionalityTransactionController> init = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            jdbcClient = JDBCClient.createShared(VertxUtilties.INSTANCE, new JsonObject(sysKB.get("persistence").toJson()));
            jdbcClient.getConnection(result -> {
                try {
                    if (result.failed()) {
                        throw result.cause();
                    }
                    sqlConnection = result.result();
                    init.complete(this);
                } catch(Throwable throwable) {
                    init.completeExceptionally(throwable);
                }
            });
        }, asyncExecutor);
        return init;
    }


    private final DataRepresentation toDataRepresentation(ResultSet resultSet) {
        DataRepresentation results = Tools.FACTORY_DATA_REPRESENTATION.create();
        Linq4j.asEnumerable(resultSet.getResults()).forEach(it -> results.add(Tools.FACTORY_DATA_REPRESENTATION.fromJson(it.encode())));
        TextSeries columnNames = Tools.FACTORY_TEXT_SERIES.create(Linq4j.asEnumerable(resultSet.getColumnNames()).select(it -> it.toString()).toList());
        DataRepresentation rows = Tools.FACTORY_DATA_REPRESENTATION.create();
        int cols = columnNames.size();
        for (int j = 0; j < results.length(); j++) {
            DataRepresentation result = results.get(j);
            DataRepresentation row = Tools.FACTORY_DATA_REPRESENTATION.create();
            for (int i = 0; i < cols; i++) {
                row.add(columnNames.get(i), result.get(i));
            }
            rows.add(row);
        }
        return rows;
    }

    @Override
    public final CompletableFuture<Void> close(Executor asyncExecutor) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            sqlConnection.close(result -> {
                try {
                    if (result.failed()) {
                        throw result.cause();
                    }
                    jdbcClient.close();
                    sqlConnection = null;
                    jdbcClient = null;
                    future.complete(result.result());
                } catch(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        }, asyncExecutor);
        return future;
    }

    @Override
    public final CompletableFuture<Void> initTransaction(Executor asyncExecutor) {
        if(inTransaction) {
            return END;
        }
        final CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            sqlConnection.setAutoCommit(true, result -> {
                try {
                    if(result.failed()) {
                        throw result.cause();
                    }
                    inTransaction = true;
                    future.complete(result.result());
                } catch(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        }, asyncExecutor);
        return future;
    }

    @Override
    public final CompletableFuture<Void> commitTransaction(Executor asyncExecutor) {
        if(!inTransaction) {
            return END;
        }
        final CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            sqlConnection.commit(result -> {
                try {
                    if(result.failed()) {
                        throw result.cause();
                    }
                    inTransaction = false;
                    future.complete(result.result());
                } catch(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        }, asyncExecutor);
        return future;
    }

    @Override
    public final CompletableFuture<Void> rollbackTransaction(Executor asyncExecutor) {
        if(!inTransaction) {
            return END;
        }
        final CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            sqlConnection.rollback(result -> {
                try {
                    if (result.failed()) {
                        throw result.cause();
                    }
                    inTransaction = false;
                    future.complete(null);
                } catch(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        }, asyncExecutor);
        return future;
    }

    @Override
    public final boolean isInTransaction() {
        return inTransaction;
    }

    @Override
    public final CompletableFuture<DataRepresentation> query(String sql, Executor asyncExecutor) {
        final CompletableFuture<DataRepresentation> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            sqlConnection.query(sql, result -> {
                try {
                    if (result.failed()) {
                        throw result.cause();
                    }
                    future.complete(toDataRepresentation(result.result()));
                } catch(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        }, asyncExecutor);
        return future;
    }

    @Override
    public final CompletableFuture<OperationResult> update(String sql, Executor asyncExecutor) {
        final CompletableFuture<OperationResult> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            sqlConnection.update(sql, result -> {
                try {
                    if (result.failed()) {
                        throw result.cause();
                    }
                    future.complete(OperationResult.create((long)result.result().getUpdated(), Tools.FACTORY_TEXT_SERIES.fromJson(result.result().getKeys().toString())));
                } catch(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        }, asyncExecutor);
        return future;
    }
}