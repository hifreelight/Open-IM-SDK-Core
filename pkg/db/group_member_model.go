package db

import (
	"errors"
	"fmt"
	"open_im_sdk/pkg/constant"
	"open_im_sdk/pkg/db/model_struct"
	"open_im_sdk/pkg/log"
	"open_im_sdk/pkg/utils"
)

func (d *DataBase) GetGroupMemberInfoByGroupIDUserID(groupID, userID string) (*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupMember model_struct.LocalGroupMember
	return &groupMember, utils.Wrap(d.conn.Where("group_id = ? AND user_id = ?",
		groupID, userID).Take(&groupMember).Error, "GetGroupMemberInfoByGroupIDUserID failed")
}

func (d *DataBase) GetAllGroupMemberList() ([]model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupMemberList []model_struct.LocalGroupMember
	return groupMemberList, utils.Wrap(d.conn.Find(&groupMemberList).Error, "GetAllGroupMemberList failed")
}

func (d *DataBase) GetGroupMemberCount(groupID string) (uint32, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var count int64
	err := d.conn.Model(&model_struct.LocalGroupMember{}).Where("group_id = ? ", groupID).Count(&count).Error
	return uint32(count), utils.Wrap(err, "GetGroupMemberCount failed")
}

func (d *DataBase) GetGroupSomeMemberInfo(groupID string, userIDList []string) ([]*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupMemberList []model_struct.LocalGroupMember
	err := d.conn.Where("group_id = ? And user_id IN ? ", groupID, userIDList).Find(&groupMemberList).Error
	var transfer []*model_struct.LocalGroupMember
	for _, v := range groupMemberList {
		v1 := v
		transfer = append(transfer, &v1)
	}
	return transfer, utils.Wrap(err, "GetGroupMemberListByGroupID failed ")
}
func (d *DataBase) GetGroupAdminID(groupID string) ([]string, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var adminIDList []string
	return adminIDList, utils.Wrap(d.conn.Model(&model_struct.LocalGroupMember{}).Select("user_id").Where("group_id = ? And role_level = ?", groupID, constant.GroupAdmin).Find(&adminIDList).Error, "")
}

func (d *DataBase) GetGroupMemberListByGroupID(groupID string) ([]*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupMemberList []model_struct.LocalGroupMember
	err := d.conn.Where("group_id = ? ", groupID).Find(&groupMemberList).Error
	var transfer []*model_struct.LocalGroupMember
	for _, v := range groupMemberList {
		v1 := v
		transfer = append(transfer, &v1)
	}
	return transfer, utils.Wrap(err, "GetGroupMemberListByGroupID failed ")
}
func (d *DataBase) GetGroupMemberListSplit(groupID string, filter int32, offset, count int) ([]*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupMemberList []model_struct.LocalGroupMember
	var err error
	if filter == 0 {
		err = d.conn.Where("group_id = ? And role_level > ?", groupID, filter).Order("role_level DESC").Order("join_time ASC").Offset(offset).Limit(count).Find(&groupMemberList).Error
	} else if filter == constant.GroupOrdinaryUsers || filter == constant.GroupOwner || filter == constant.GroupAdmin {
		err = d.conn.Where("group_id = ? And role_level = ?", groupID, filter).Order("join_time ASC").Offset(offset).Limit(count).Find(&groupMemberList).Error
	} else if filter == constant.GroupAdminAndOrdinaryUsers {
		err = d.conn.Where("group_id = ? And ( role_level = 1 OR role_level = 3 ) ", groupID).Order("role_level DESC").Order("join_time ASC").Offset(offset).Limit(count).Find(&groupMemberList).Error
	} else {
		return nil, errors.New("filter args failed")
	}

	var transfer []*model_struct.LocalGroupMember
	for _, v := range groupMemberList {
		v1 := v
		transfer = append(transfer, &v1)
	}
	return transfer, utils.Wrap(err, "GetGroupMemberListSplit failed ")
}

func (d *DataBase) GetGroupMemberOwnerAndAdmin(groupID string) ([]*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupMemberList []model_struct.LocalGroupMember
	err := d.conn.Where("group_id = ? And role_level > ?", groupID, constant.GroupOrdinaryUsers).Order("join_time DESC").Find(&groupMemberList).Error
	var transfer []*model_struct.LocalGroupMember
	for _, v := range groupMemberList {
		v1 := v
		transfer = append(transfer, &v1)
	}
	return transfer, utils.Wrap(err, "GetGroupMemberListSplit failed ")
}

func (d *DataBase) GetGroupMemberListSplitByJoinTimeFilter(groupID string, offset, count int, joinTimeBegin, joinTimeEnd int64, userIDList []string) ([]*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupMemberList []model_struct.LocalGroupMember
	var err error
	if len(userIDList) == 0 {
		err = d.conn.Where("group_id = ? And join_time  between ? and ? ", groupID, joinTimeBegin, joinTimeEnd).Order("join_time DESC").Offset(offset).Limit(count).Find(&groupMemberList).Error
	} else {
		err = d.conn.Where("group_id = ? And join_time  between ? and ? And user_id NOT IN ?", groupID, joinTimeBegin, joinTimeEnd, userIDList).Order("join_time DESC").Offset(offset).Limit(count).Find(&groupMemberList).Error
	}
	var transfer []*model_struct.LocalGroupMember
	for _, v := range groupMemberList {
		v1 := v
		transfer = append(transfer, &v1)
	}
	return transfer, utils.Wrap(err, "GetGroupMemberListSplitByJoinTimeFilter failed ")
}

func (d *DataBase) GetGroupOwnerAndAdminByGroupID(groupID string) ([]*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupMemberList []model_struct.LocalGroupMember
	err := d.conn.Where("group_id = ?  AND role_level > ?", groupID, constant.GroupOrdinaryUsers).Find(&groupMemberList).Error
	var transfer []*model_struct.LocalGroupMember
	for _, v := range groupMemberList {
		v1 := v
		transfer = append(transfer, &v1)
	}
	return transfer, utils.Wrap(err, "GetGroupMemberListByGroupID failed ")
}

func (d *DataBase) GetGroupMemberUIDListByGroupID(groupID string) (result []string, err error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var g model_struct.LocalGroupMember
	g.GroupID = groupID
	err = d.conn.Model(&g).Pluck("user_id", &result).Error
	return result, utils.Wrap(err, "GetGroupMemberListByGroupID failed ")
}

func (d *DataBase) GetGroupMemberByUserID(groupID, userID string) (*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	groupMember := model_struct.LocalGroupMember{}
	return &groupMember, utils.Wrap(d.conn.Where("group_id=? and user_id=?", groupID, userID).First(&groupMember).Error, "GetGroupMemberByUserID failed")
}

func (d *DataBase) InsertGroupMember(groupMember *model_struct.LocalGroupMember) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return utils.Wrap(d.conn.Create(groupMember).Error, "")
}

//func (d *DataBase) BatchInsertMessageList(MessageList []*model_struct.LocalChatLog) error {
//	if MessageList == nil {
//		return nil
//	}
//	d.mRWMutex.Lock()
//	defer d.mRWMutex.Unlock()
//	return utils.Wrap(d.conn.Create(MessageList).Error, "BatchInsertMessageList failed")
//}

func (d *DataBase) BatchInsertGroupMember(groupMemberList []*model_struct.LocalGroupMember) error {
	if groupMemberList == nil {
		return errors.New("nil")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return utils.Wrap(d.conn.Create(groupMemberList).Error, "BatchInsertMessageList failed")
}

func (d *DataBase) DeleteGroupMember(groupID, userID string) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	groupMember := model_struct.LocalGroupMember{}
	return d.conn.Where("group_id=? and user_id=?", groupID, userID).Delete(&groupMember).Error
}

func (d *DataBase) DeleteGroupAllMembers(groupID string) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	groupMember := model_struct.LocalGroupMember{}
	return d.conn.Where("group_id=? ", groupID).Delete(&groupMember).Error
}

func (d *DataBase) UpdateGroupMember(groupMember *model_struct.LocalGroupMember) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	t := d.conn.Model(groupMember).Select("*").Updates(*groupMember)
	if t.RowsAffected == 0 {
		return utils.Wrap(errors.New("RowsAffected == 0"), "no update")
	}
	return utils.Wrap(t.Error, "")
}

func (d *DataBase) GetGroupMemberInfoIfOwnerOrAdmin() ([]*model_struct.LocalGroupMember, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var ownerAndAdminList []*model_struct.LocalGroupMember
	groupList, err := d.GetJoinedGroupList()
	if err != nil {
		return nil, utils.Wrap(err, "")
	}
	for _, v := range groupList {

		memberList, err := d.GetGroupOwnerAndAdminByGroupID(v.GroupID)
		if err != nil {
			return nil, utils.Wrap(err, "")
		}
		ownerAndAdminList = append(ownerAndAdminList, memberList...)
	}
	return ownerAndAdminList, nil
}

func (d *DataBase) SearchGroupMembers(keyword string, groupID string, isSearchMemberNickname, isSearchUserID bool, offset, count int) (result []*model_struct.LocalGroupMember, err error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	if !isSearchMemberNickname && !isSearchUserID {
		return nil, errors.New("args failed")
	}

	var countCon int
	var condition string
	if isSearchUserID {
		condition = fmt.Sprintf("user_id like %q ", "%"+keyword+"%")
		countCon++
	}
	if isSearchMemberNickname {
		if countCon > 0 {
			condition += "or "
		}
		condition += fmt.Sprintf("nickname like %q ", "%"+keyword+"%")
	}

	var groupMemberList []model_struct.LocalGroupMember
	if groupID != "" {
		condition = "( " + condition + " ) "
		condition += " and group_id IN ? "
		log.Debug("", "subCondition SearchGroupMembers ", condition)
		err = d.conn.Where(condition, []string{groupID}).Order("join_time DESC").Offset(offset).Limit(count).Find(&groupMemberList).Error
	} else {
		log.Debug("", "subCondition SearchGroupMembers ", condition)
		err = d.conn.Where(condition).Order("join_time DESC").Offset(offset).Limit(count).Find(&groupMemberList).Error
		log.Debug("", "subCondition SearchGroupMembers ", condition, len(groupMemberList))
	}

	for _, v := range groupMemberList {
		v1 := v
		result = append(result, &v1)
	}
	return result, err
}
