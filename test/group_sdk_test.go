package test

import (
	"fmt"
	"open_im_sdk/pkg/db"
	"open_im_sdk/pkg/log"
	"testing"
)

func TestIsInGroup(t *testing.T) {

	userID := "4010114391"
	groupID := "1624423272"
	DataDir := "../ws_wrapper/cmd"
	db, err := db.NewDataBase(userID, DataDir)
	if err != nil {

		log.Error("NewDataBase failed ", err.Error())
		return
	}
	user, _ := db.GetLoginUser()
	fmt.Printf("user: %+v\n", user)
	member, _ := db.GetGroupMemberByUserID(groupID, userID)

	fmt.Printf("member: %+v\n", member)

}
