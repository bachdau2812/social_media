package com.dauducbach.clone.modules.user.entity;

import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Table("user_details")
public class UserDetails {
    @Id
    String userId;
    String username;
    String fullName;
    LocalDate dob;
    String hometown;
    String livingIn;
    String sex;

    /**
     * Lưu hobbieList dưới dạng JSON string trong database
     * MySQL không hỗ trợ array columns
     */
    String hobbyList;

    /**
     * Getter cho hobbieList - convert JSON string thành List<String>
     */
    public List<String> getHobbyList() {
        if (hobbyList == null || hobbyList.trim().isEmpty()) {
            return new ArrayList<>();
        }
        try {
            return GsonUtils.getGson().fromJson(hobbyList, new TypeToken<List<String>>(){}.getType());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    /**
     * Setter cho hobbieList - convert List<String> thành JSON string
     */
    public void setHobbyList(List<String> hobbyList) {
        if (hobbyList == null || hobbyList.isEmpty()) {
            this.hobbyList = "[]";
        } else {
            try {
                this.hobbyList = GsonUtils.getGson().toJson(hobbyList);
            } catch (Exception e) {
                this.hobbyList = "[]";
            }
        }
    }
}
