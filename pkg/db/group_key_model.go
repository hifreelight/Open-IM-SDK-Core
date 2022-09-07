package db

import (
	"errors"
	"open_im_sdk/pkg/db/model_struct"
	"open_im_sdk/pkg/utils"
)

func (d *DataBase) GetAllGroupKeyList() ([]model_struct.LocalGroupKey, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupKeyList []model_struct.LocalGroupKey
	return groupKeyList, utils.Wrap(d.conn.Find(&groupKeyList).Error, "GetAllGroupKeyList failed")
}

// 需求是只保留最近三天， 这里取最多100条
func (d *DataBase) GetGroupKeyListByGroupID(groupID string) ([]*model_struct.LocalGroupKey, error) {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	var groupKeyList []model_struct.LocalGroupKey
	var count int = 100
	err := d.conn.Where("group_id = ? ", groupID).Order("create_time DESC").Limit(count).Find(&groupKeyList).Error
	var transfer []*model_struct.LocalGroupKey
	for _, v := range groupKeyList {
		v1 := v
		transfer = append(transfer, &v1)
	}
	return transfer, utils.Wrap(err, "GetGroupMemberListByGroupID failed ")
}

func (d *DataBase) InsertGroupKey(groupKey *model_struct.LocalGroupKey) error {
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return utils.Wrap(d.conn.Create(groupKey).Error, "")
}

func (d *DataBase) BatchInsertGroupKey(groupKeyList []*model_struct.LocalGroupKey) error {
	if groupKeyList == nil {
		return errors.New("nil")
	}
	d.mRWMutex.Lock()
	defer d.mRWMutex.Unlock()
	return utils.Wrap(d.conn.Create(groupKeyList).Error, "BatchInsertGroupKeyList failed")
}
