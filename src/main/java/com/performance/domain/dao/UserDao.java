package com.performance.domain.dao;

import java.util.List;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.performance.domain.entity.UserHobby;
import com.performance.domain.entity.UserInfo;

@Repository
public class UserDao {

    private JdbcTemplate jdbcTemplate;
    
    public UserDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void insertUserInfo (UserInfo entity) {
        String sql = "INSERT INTO user_info (last_name, first_name, prefectures, city, blood_type)";
        sql = sql + " VALUES (";
        sql = sql + "'" + entity.getLastName() + "', ";
        sql = sql + "'" + entity.getFirstName() + "', ";
        sql = sql + "'" + entity.getPrefectures() + "', ";
        sql = sql + "'" + entity.getCity() + "', ";
        sql = sql + "'" + entity.getBloodType() + "')";
        jdbcTemplate.execute(sql);
    }

    @Transactional
    public void batchInsertUserInfo (List<UserInfo> entities) {
        String sql = "INSERT INTO user_info (id, last_name, first_name, prefectures, city, blood_type) values (?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, entities.get(i).getId());
                ps.setString(2, entities.get(i).getLastName());
                ps.setString(3, entities.get(i).getFirstName());
                ps.setString(4, entities.get(i).getPrefectures());
                ps.setString(5, entities.get(i).getCity());
                ps.setString(6, entities.get(i).getBloodType());
            }
        
            @Override
            public int getBatchSize() {
                return entities.size();
            }
        });
    }
    
    @Transactional
    public void insertUserHobby (UserHobby entity) {
        String sql = "INSERT INTO user_hobby (id, hobby1, hobby2, hobby3, hobby4, hobby5)";
        sql = sql + " VALUES (";
        sql = sql + "'" + entity.getId() + "', ";
        sql = sql + "'" + entity.getHobby1() + "', ";
        sql = sql + "'" + entity.getHobby2() + "', ";
        sql = sql + "'" + entity.getHobby3() + "', ";
        sql = sql + "'" + entity.getHobby4() + "', ";
        sql = sql + "'" + entity.getHobby5() + "')";
        jdbcTemplate.execute(sql);
    }

    @Transactional
    public void batchInsertUserHobby (List<UserHobby> entities) {
        String sql = "INSERT INTO user_hobby (id, hobby1, hobby2, hobby3, hobby4, hobby5) values (?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, entities.get(i).getId());
                ps.setString(2, entities.get(i).getHobby1());
                ps.setString(3, entities.get(i).getHobby2());
                ps.setString(4, entities.get(i).getHobby3());
                ps.setString(5, entities.get(i).getHobby4());
                ps.setString(6, entities.get(i).getHobby5());
            }
        
            @Override
            public int getBatchSize() {
                return entities.size();
            }
        });
    }
    
    public String selectId(UserInfo entity) {
        String sql = "SELECT id ";
        sql = sql + "FROM user_info ";
        sql = sql + "WHERE last_name || first_name = " + "'" + entity.getLastName() + entity.getFirstName() + "'";
        sql = sql + " ORDER BY id desc";
        sql = sql + " LIMIT 1";
        return jdbcTemplate.queryForObject(sql, String.class);
    }

    public List<UserInfo> searchUserInfo() {
        String sql = "SELECT id, last_name, first_name, prefectures, city, blood_type ";
        sql = sql + "FROM user_info ";
        sql = sql + "WHERE last_name || first_name <> " + "'試験太郎'";
        sql = sql + " ORDER BY id";
        RowMapper<UserInfo> mapper = new BeanPropertyRowMapper<UserInfo>(UserInfo.class);
        return jdbcTemplate.query(sql, mapper);
    }

    public List<UserHobby> searchUserHobby(UserHobby targetUserHobby) {
        String sql = "SELECT id, hobby1, hobby2, hobby3, hobby4, hobby5 ";
        sql = sql + "FROM user_hobby ";
        sql = sql + "WHERE id  <> ? ";
        sql = sql + " ORDER BY id";
        RowMapper<UserHobby> mapper = new BeanPropertyRowMapper<UserHobby>(UserHobby.class);
        return jdbcTemplate.query(sql, mapper, targetUserHobby.getId());
    }
    
    public UserInfo getTargetUserInfo() {
        String sql = "SELECT id, last_name, first_name, prefectures, city, blood_type ";
        sql = sql + "FROM user_info ";
        sql = sql + "WHERE last_name = " + "'試験'";
        sql = sql + "AND first_name = " + "'太郎'";
        RowMapper<UserInfo> mapper = new BeanPropertyRowMapper<UserInfo>(UserInfo.class);
        return jdbcTemplate.queryForObject(sql, mapper);
    }
    
    public UserHobby getTargetUserHobby(UserInfo userInfo) {
        String sql = "SELECT id, hobby1, hobby2, hobby3, hobby4, hobby5 ";
        sql = sql + "FROM user_hobby ";
        sql = sql + "WHERE id = ? ";
        RowMapper<UserHobby> mapper = new BeanPropertyRowMapper<UserHobby>(UserHobby.class);
        return jdbcTemplate.queryForObject(sql, mapper, userInfo.getId());
    }
    
    public int searchCount() {
        String sql = "SELECT COUNT(*) FROM user_info";
        
        return jdbcTemplate.queryForObject(sql, Integer.class);
    }

    public void truncateUserInfo() {
        String sql = "TRUNCATE TABLE user_info";
        jdbcTemplate.execute(sql);
    }
    
    public void truncateUserHobby() {
        String sql = "TRUNCATE TABLE user_hobby";
        jdbcTemplate.execute(sql);
    }
    
}
